package daemon

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/Hittlert/TGX/core/downloader"
	"github.com/Hittlert/TGX/pkg/spool"
	"github.com/Hittlert/TGX/pkg/writeback"
	"github.com/flytam/filenamify"
)

type ResolvedMedia struct {
	File downloader.File
	Name string
	Size int64
	DCID int
	Date int64
}

type MediaAccess interface {
	Resolve(context.Context, string, int) (ResolvedMedia, error)
}

type taskResolver struct {
	access     MediaAccess
	tempRoot   string
	outputRoot string
	spool      spool.Store
	wbQueue    *writeback.Queue
}

func newTaskResolver(access MediaAccess, tempRoot, outputRoot string, optArgs ...any) *taskResolver {
	r := &taskResolver{access: access, tempRoot: tempRoot, outputRoot: outputRoot}
	for _, arg := range optArgs {
		switch v := arg.(type) {
		case spool.Store:
			r.spool = v
		case *writeback.Queue:
			r.wbQueue = v
		}
	}
	return r
}

func (r *taskResolver) Resolve(ctx context.Context, task *Task) (taskElement, error) {
	request := task.Request()
	media, err := r.access.Resolve(ctx, normalizePeer(request.Peer), request.MessageID)
	if err != nil {
		return nil, err
	}
	if media.File == nil || media.Size <= 0 {
		return nil, NewTaskError("unavailable", true, fmt.Errorf("message has no downloadable media"))
	}
	task.SetResolved(media.Name, media.Size, media.DCID)

	finalPath := request.FinalPath
	if finalPath == "" && media.Name != "" {
		safeMediaName, _ := filenamify.Filenamify(media.Name, filenamify.Options{Replacement: "_"})
		if safeMediaName != "" {
			dir := filepath.Dir(request.FinalPath)
			finalPath = filepath.Join(dir, fmt.Sprintf("%d - %s", request.MessageID, safeMediaName))
			finalPath = strings.ReplaceAll(finalPath, "\\", "/")
			task.SetFinalPath(finalPath)
		}
	}

	absolute, err := safeOutputPath(r.outputRoot, finalPath)
	if err != nil {
		return nil, NewTaskError("path", false, err)
	}
	if exists, err := existingFile(absolute, media.Size); err != nil {
		return nil, NewTaskError("collision", false, err)
	} else if exists {
		verifiedSHA, err := verifyFinalFileIdentity(absolute, media.Size, "", request.ID)
		if err != nil {
			return nil, NewTaskError("collision", false, err)
		}
		return &existingElement{task: task, file: media.File, path: finalPath, sha: verifiedSHA}, nil
	}

	if r.spool != nil && r.wbQueue != nil {
		spoolElem, err := newSpoolFileElement(task, media.File, r.outputRoot, media.Date, r.spool, r.wbQueue)
		if err != nil {
			return nil, NewTaskError("spool", false, err)
		}
		return spoolElem, nil
	}

	element, err := newFileElement(task, media.File, r.tempRoot, r.outputRoot, media.Date)
	if err != nil {
		return nil, NewTaskError("filesystem", false, err)
	}
	return element, nil
}

func normalizePeer(peer string) string {
	peer = strings.TrimPrefix(strings.TrimSpace(peer), "@")
	if strings.HasPrefix(peer, "-100") && len(peer) > 4 {
		return peer[4:]
	}
	return strings.TrimPrefix(peer, "-")
}
