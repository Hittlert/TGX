package cmd

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-faster/errors"
	"github.com/gotd/td/telegram"
	"github.com/ivanpirog/coloredcobra"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/Hittlert/TGX/core/logctx"
	"github.com/Hittlert/TGX/core/storage"
	tclientcore "github.com/Hittlert/TGX/core/tclient"
	"github.com/Hittlert/TGX/core/util/fsutil"
	"github.com/Hittlert/TGX/core/util/logutil"
	"github.com/Hittlert/TGX/pkg/consts"
	"github.com/Hittlert/TGX/pkg/kv"
	"github.com/Hittlert/TGX/pkg/tclient"
)

var (
	defaultBoltPath = filepath.Join(consts.DataDir, "data")

	DefaultLegacyStorage = map[string]string{
		kv.DriverTypeKey: kv.DriverLegacy.String(),
		"path":           filepath.Join(consts.DataDir, "data.kv"),
	}
	DefaultBoltStorage = map[string]string{
		kv.DriverTypeKey: kv.DriverBolt.String(),
		"path":           defaultBoltPath,
	}
)

func New() *cobra.Command {
	cobra.EnableTraverseRunHooks = true

	cmd := &cobra.Command{
		Use:           "tgx",
		Aliases:       []string{"tg-downloader", "tdl"},
		Short:         "TGX - Next-Generation Telegram Media Downloader & Streaming Archive Engine",
		SilenceErrors: true,
		SilenceUsage:  true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			debug, level := viper.GetBool(consts.FlagDebug), zap.InfoLevel
			if debug {
				level = zap.DebugLevel
			}
			cmd.SetContext(logctx.With(cmd.Context(),
				logutil.New(level, filepath.Join(consts.LogPath, "latest.log"))))

			if cmd.Flags().Changed("namespace") {
				if nsVal, _ := cmd.Flags().GetString("namespace"); nsVal != "" {
					viper.Set(consts.FlagNamespace, nsVal)
				}
			}
			ns := viper.GetString(consts.FlagNamespace)
			if ns != "" {
				logctx.From(cmd.Context()).Info("Namespace", zap.String("namespace", ns))
			}

			storageMap := viper.GetStringMapString(consts.FlagStorage)
			if storagePath, _ := cmd.Flags().GetString("storage-path"); storagePath != "" {
				storageMap = map[string]string{
					kv.DriverTypeKey: kv.DriverBolt.String(),
					"path":           storagePath,
				}
			} else if !cmd.Flags().Lookup(consts.FlagStorage).Changed && !fsutil.PathExists(defaultBoltPath) {
				if err := migrateLegacyToBolt(); err != nil {
					return errors.Wrap(err, "migrate legacy to bolt")
				}
			}

			stg, err := kv.NewWithMap(storageMap)
			if err != nil {
				return errors.Wrap(err, "create kv storage")
			}

			cmd.SetContext(kv.With(cmd.Context(), stg))
			return nil
		},
		PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
			_ = logctx.From(cmd.Context()).Sync()
			if stg := kv.From(cmd.Context()); stg != nil {
				return stg.Close()
			}
			return nil
		},
	}

	coloredcobra.Init(&coloredcobra.Config{
		RootCmd:         cmd,
		Headings:        coloredcobra.HiCyan + coloredcobra.Bold + coloredcobra.Underline,
		Commands:        coloredcobra.HiGreen + coloredcobra.Bold,
		CmdShortDescr:   coloredcobra.None,
		ExecName:        coloredcobra.Bold,
		Flags:           coloredcobra.Bold + coloredcobra.Yellow,
		FlagsDataType:   coloredcobra.Blue,
		FlagsDescr:      coloredcobra.None,
		Aliases:         coloredcobra.None,
		Example:         coloredcobra.None,
		NoExtraNewlines: true,
		NoBottomNewline: true,
	})

	// Register Core TGX Commands
	cmd.AddCommand(
		NewDownload(),
		NewDaemon(),
		NewLogin(),
		NewHealthcheck(),
		NewVersion(),
	)

	cmd.PersistentFlags().String("storage-path", "", "storage directory path")
	cmd.PersistentFlags().String("namespace", "", "namespace for Telegram session")
	cmd.PersistentFlags().StringToString(consts.FlagStorage,
		DefaultBoltStorage,
		fmt.Sprintf("storage options, format: type=driver,key1=value1,key2=value2. Available drivers: [%s]",
			strings.Join(kv.DriverNames(), ",")))

	cmd.PersistentFlags().String(consts.FlagProxy, "", "proxy address, format: protocol://username:password@host:port")
	cmd.PersistentFlags().StringP(consts.FlagNamespace, "n", "default", "namespace for Telegram session")
	cmd.PersistentFlags().Bool(consts.FlagDebug, false, "enable debug mode")

	cmd.PersistentFlags().IntP(consts.FlagThreads, "t", 4, "max threads for transfer one item")
	cmd.PersistentFlags().IntP(consts.FlagLimit, "l", 2, "max number of concurrent tasks")
	cmd.PersistentFlags().Int(consts.FlagPoolSize, 8, "specify the size of the DC pool, zero means infinity")
	cmd.PersistentFlags().Duration(consts.FlagDelay, 0, "delay between each task, zero means no delay")

	cmd.PersistentFlags().String(consts.FlagNTP, "", "ntp server host, if not set, use system time")
	cmd.PersistentFlags().Duration(consts.FlagReconnectTimeout, 5*time.Minute, "Telegram client reconnection backoff timeout, infinite if set to 0")
	cmd.PersistentFlags().Bool(consts.FlagDisableProgressPS, false, "disable progress ps, which may cause display issues in some terminals")

	_ = cmd.RegisterFlagCompletionFunc(consts.FlagNamespace, func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		engine := kv.From(cmd.Context())
		ns, err := engine.Namespaces()
		if err != nil {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		return ns, cobra.ShellCompDirectiveNoFileComp
	})

	_ = viper.BindPFlags(cmd.PersistentFlags())

	viper.SetEnvPrefix("tgx")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	cmd.TraverseChildren = true
	return cmd
}

type completeFunc func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective)

func completeExtFiles(ext ...string) completeFunc {
	return func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		files := make([]string, 0)
		for _, e := range ext {
			f, err := filepath.Glob(toComplete + "*." + e)
			if err != nil {
				return nil, cobra.ShellCompDirectiveDefault
			}
			files = append(files, f...)
		}

		return files, cobra.ShellCompDirectiveFilterDirs
	}
}

func tOptions(ctx context.Context) (tclient.Options, error) {
	kvd, err := kv.From(ctx).Open(viper.GetString(consts.FlagNamespace))
	if err != nil {
		return tclient.Options{}, errors.Wrap(err, "open kv storage")
	}
	o := tclient.Options{
		KV:               kvd,
		Proxy:            viper.GetString(consts.FlagProxy),
		NTP:              viper.GetString(consts.FlagNTP),
		ReconnectTimeout: viper.GetDuration(consts.FlagReconnectTimeout),
		UpdateHandler:    nil,
	}

	return o, nil
}

func tRunWithUpdateHandler(ctx context.Context, handler telegram.UpdateHandler, f func(ctx context.Context, c *telegram.Client, kvd storage.Storage) error, middlewares ...telegram.Middleware) error {
	o, err := tOptions(ctx)
	if err != nil {
		return errors.Wrap(err, "build telegram options")
	}
	o.UpdateHandler = handler

	client, err := tclient.New(ctx, o, false, middlewares...)
	if err != nil {
		return errors.Wrap(err, "create client")
	}

	return tclientcore.RunWithAuth(ctx, client, func(ctx context.Context) error {
		return f(ctx, client, o.KV)
	})
}

func tRun(ctx context.Context, f func(ctx context.Context, c *telegram.Client, kvd storage.Storage) error, middlewares ...telegram.Middleware) error {
	return tRunWithUpdateHandler(ctx, nil, f, middlewares...)
}

func migrateLegacyToBolt() (rerr error) {
	legacy, err := kv.NewWithMap(DefaultLegacyStorage)
	if err != nil {
		return errors.Wrap(err, "create legacy kv storage")
	}
	defer multierr.AppendInvoke(&rerr, multierr.Close(legacy))

	bolt, err := kv.NewWithMap(DefaultBoltStorage)
	if err != nil {
		return errors.Wrap(err, "create bolt kv storage")
	}
	defer multierr.AppendInvoke(&rerr, multierr.Close(bolt))

	meta, err := legacy.MigrateTo()
	if err != nil {
		return errors.Wrap(err, "migrate legacy to bolt")
	}

	return bolt.MigrateFrom(meta)
}
