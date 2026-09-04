package dl

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/AlecAivazis/survey/v2"
	"github.com/fatih/color"
	"github.com/go-faster/errors"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/tg"
	"github.com/spf13/viper"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/Hittlert/TGX/core/dcpool"
	"github.com/Hittlert/TGX/core/logctx"
	"github.com/Hittlert/TGX/core/storage"
	"github.com/Hittlert/TGX/core/tclient"
	"github.com/Hittlert/TGX/core/transfer"
	"github.com/Hittlert/TGX/pkg/consts"
	"github.com/Hittlert/TGX/pkg/key"
	"github.com/Hittlert/TGX/pkg/prog"
	"github.com/Hittlert/TGX/pkg/tmessage"
	"github.com/Hittlert/TGX/pkg/utils"
	"golang.org/x/sync/errgroup"
)

type Options struct {
	Dir        string
	RewriteExt bool
	SkipSame   bool
	Template   string
	URLs       []string
	Files      []string
	Include    []string
	Exclude    []string
	Desc       bool
	Takeout    bool
	Group      bool // auto detect grouped message

	// resume opts
	Continue, Restart bool

	// serve
	Serve bool
	Port  int
}

type parser struct {
	Data   []string
	Parser tmessage.ParseSource
}

func Run(ctx context.Context, c *telegram.Client, kvd storage.Storage, opts Options) (rerr error) {
	pool := dcpool.NewPool(c,
		int64(viper.GetInt(consts.FlagPoolSize)),
		tclient.NewDefaultMiddlewares(ctx, viper.GetDuration(consts.FlagReconnectTimeout))...)
	defer multierr.AppendInvoke(&rerr, multierr.Close(pool))

	parsers := []parser{
		{Data: opts.URLs, Parser: tmessage.FromURL(ctx, pool, kvd, opts.URLs)},
		{Data: opts.Files, Parser: tmessage.FromFile(ctx, pool, kvd, opts.Files, true)},
	}
	dialogs, err := collectDialogs(parsers)
	if err != nil {
		return err
	}
	logctx.From(ctx).Debug("Collect dialogs",
		zap.Any("dialogs", dialogs))

	if opts.Serve {
		return serve(ctx, kvd, pool, dialogs, opts.Port, opts.Takeout)
	}

	manager := peers.Options{Storage: storage.NewPeers(kvd)}.Build(pool.Default(ctx))

	it, err := newIter(pool, manager, dialogs, opts, viper.GetDuration(consts.FlagDelay))
	if err != nil {
		return err
	}

	if !opts.Restart {
		// resume download and ask user to continue
		if err = resume(ctx, kvd, it, !opts.Continue); err != nil {
			return err
		}
	} else {
		color.Yellow("Restart download by 'restart' flag")
	}

	defer func() { // save progress
		if rerr != nil { // download is interrupted
			multierr.AppendInto(&rerr, saveProgress(ctx, kvd, it))
		} else { // if finished, we should clear resume key
			multierr.AppendInto(&rerr, kvd.Delete(ctx, key.Resume(it.Fingerprint())))
		}
	}()

	dlProgress := prog.New(utils.Byte.FormatBinaryBytes)
	dlProgress.SetNumTrackersExpected(it.Total())
	if !viper.GetBool(consts.FlagDisableProgressPS) {
		prog.EnablePS(ctx, dlProgress)
	}

	limit := viper.GetInt(consts.FlagLimit)
	if limit <= 0 {
		limit = 4
	}
	threads := viper.GetInt(consts.FlagThreads)
	if threads <= 0 {
		threads = 8
	}

	transferMgr := transfer.NewTransferManager(transfer.Options{
		FileConcurrency: limit,
		MaxFileThreads:  threads,
		MaxDataInFlight: int64(viper.GetInt(consts.FlagPoolSize)),
	})
	p := newProgress(dlProgress, it, opts)

	logctx.From(ctx).Info("Start download",
		zap.String("dir", opts.Dir),
		zap.Bool("rewrite_ext", opts.RewriteExt),
		zap.Bool("skip_same", opts.SkipSame),
		zap.Int("threads", threads),
		zap.Int("limit", limit))

	color.Green("All files will be downloaded to '%s' dir", opts.Dir)

	go dlProgress.Render()
	defer func() {
		prog.Wait(ctx, dlProgress)

		// Notify user if any messages were skipped due to deletion
		// This is deferred to ensure it shows after progress rendering completes
		if skipped := it.SkippedDeleted(); skipped > 0 {
			deletedIDs := it.DeletedIDs()
			if len(deletedIDs) <= 5 {
				// Show all IDs if 5 or fewer
				color.Yellow("⚠️  %d message(s) were skipped because they were deleted: %v", skipped, deletedIDs)
			} else {
				// Show first 5 and indicate there are more
				color.Yellow("⚠️  %d message(s) were skipped because they were deleted: %v... and %d more",
					skipped, deletedIDs[:5], len(deletedIDs)-5)
			}
		}
	}()

	return runDownloadLoop(ctx, it, pool, transferMgr, limit, p)
}

type elementIterator interface {
	Next(ctx context.Context) bool
	Value() *iterElem
	Err() error
}

type progressTracker interface {
	OnAdd(elem *iterElem)
	OnDownload(elem *iterElem, downloaded, total int64)
	OnDone(elem *iterElem, err error)
}

func runDownloadLoop(
	ctx context.Context,
	it elementIterator,
	pool dcpool.Pool,
	transferMgr *transfer.TransferManager,
	limit int,
	p progressTracker,
) error {
	g, gctx := errgroup.WithContext(ctx)
	sem := make(chan struct{}, limit)

downloadLoop:
	for it.Next(gctx) {
		elem := it.Value()
		if p != nil {
			p.OnAdd(elem)
		}

		select {
		case sem <- struct{}{}:
		case <-gctx.Done():
			break downloadLoop
		}

		curElem := elem
		g.Go(func() error {
			defer func() { <-sem }()

			var invoker tg.Invoker
			if curElem.AsTakeout() {
				invoker = pool.TakeoutInvoker(gctx, curElem.DC())
			} else {
				invoker = pool.Invoker(gctx, curElem.DC())
			}
			client := transfer.NewGatedClient(
				invoker,
				transferMgr.Gate(),
				curElem.DC(),
				pool.CDN,
			)

			_, dlErr := transferMgr.DownloadFile(
				gctx,
				client,
				curElem.Location(),
				curElem.Size(),
				curElem.To(),
				func(downloaded, total int64) {
					if p != nil {
						p.OnDownload(curElem, downloaded, total)
					}
				},
			)
			if p != nil {
				p.OnDone(curElem, dlErr)
			}
			return dlErr
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}
	return it.Err()
}

func collectDialogs(parsers []parser) ([][]*tmessage.Dialog, error) {
	var dialogs [][]*tmessage.Dialog
	for _, p := range parsers {
		d, err := tmessage.Parse(p.Parser)
		if err != nil {
			return nil, err
		}
		dialogs = append(dialogs, d)
	}
	return dialogs, nil
}

func resume(ctx context.Context, kvd storage.Storage, iter *iter, ask bool) error {
	logctx.From(ctx).Debug("Check resume key",
		zap.String("fingerprint", iter.Fingerprint()))

	b, err := kvd.Get(ctx, key.Resume(iter.Fingerprint()))
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	if len(b) == 0 { // no progress
		return nil
	}

	finished := make(map[int]struct{})
	if err = json.Unmarshal(b, &finished); err != nil {
		return err
	}

	// finished is empty, no need to resume
	if len(finished) == 0 {
		return nil
	}

	confirm := false
	resumeStr := fmt.Sprintf("Found unfinished download, continue from '%d/%d'", len(finished), iter.Total())
	if ask {
		if err = survey.AskOne(&survey.Confirm{
			Message: color.YellowString(resumeStr + "?"),
		}, &confirm); err != nil {
			return err
		}
	} else {
		color.Yellow(resumeStr)
		confirm = true
	}

	logctx.From(ctx).Debug("Resume download",
		zap.Int("finished", len(finished)))

	if !confirm {
		// clear resume key
		return kvd.Delete(ctx, key.Resume(iter.Fingerprint()))
	}

	iter.SetFinished(finished)
	return nil
}

func saveProgress(ctx context.Context, kvd storage.Storage, it *iter) error {
	finished := it.Finished()
	logctx.From(ctx).Debug("Save progress",
		zap.Int("finished", len(finished)))

	b, err := json.Marshal(finished)
	if err != nil {
		return err
	}
	return kvd.Set(ctx, key.Resume(it.Fingerprint()), b)
}
