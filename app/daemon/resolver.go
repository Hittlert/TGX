package daemon

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/flytam/filenamify"
	"github.com/Hittlert/TGX/core/downloader"
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
}

func newTaskResolver(access MediaAccess, tempRoot, outputRoot string) *taskResolver {
	return &taskResolver{access: access, tempRoot: tempRoot, outputRoot: outputRoot}
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
	if request.ExpectedSize > 0 && request.ExpectedSize != media.Size {
		return nil, NewTaskError("metadata", false, fmt.Errorf(
			"Telegram size %d does not match submitted size %d", media.Size, request.ExpectedSize,
		))
	}

	finalPath := request.FinalPath
	if media.Name != "" {
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
		return &existingElement{task: task, file: media.File, path: finalPath}, nil
	}
	element, err := newFileElement(task, media.File, r.tempRoot, r.outputRoot, media.Date)
	if err != nil {
		return nil, NewTaskError("filesystem", false, err)
	}
	return element, nil
}

func normalizePeer(peer string) string {
	peer = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(peer), "@"))
	if strings.HasPrefix(peer, "-100") && len(peer) > 4 {
		return peer[4:]
	}
	return strings.TrimPrefix(peer, "-")
}
