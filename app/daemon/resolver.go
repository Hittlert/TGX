package daemon

import (
	"context"
	"fmt"
	"os"
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
	db         *Database
}

func newTaskResolver(access MediaAccess, tempRoot, outputRoot string, optArgs ...any) *taskResolver {
	r := &taskResolver{access: access, tempRoot: tempRoot, outputRoot: outputRoot}
	for _, arg := range optArgs {
		switch v := arg.(type) {
		case *Database:
			r.db = v
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

	fileName := media.Name
	if fileName == "" {
		fileName = fmt.Sprintf("%d.bin", request.MessageID)
	}
	safeFileName, err := filenamify.Filenamify(fileName, filenamify.Options{Replacement: "_"})
	if err != nil || safeFileName == "" {
		safeFileName = fmt.Sprintf("%d.bin", request.MessageID)
	}

	finalPath := request.FinalPath
	if finalPath == "" {
		finalPath = safeFileName
	}

	absolute := filepath.Join(r.outputRoot, filepath.FromSlash(finalPath))
	if finInfo, statErr := os.Stat(absolute); statErr == nil {
		if finInfo.Size() != media.Size {
			return nil, NewTaskError("collision", false, fmt.Errorf("file size mismatch: expected %d, got %d", media.Size, finInfo.Size()))
		}
		var expectedSHA string
		if r.db != nil {
			if commitRec, err := r.db.GetTargetCommit(request.ID, ""); err == nil && commitRec != nil {
				expectedSHA = commitRec.CommittedSHA256
			}
		}
		verifiedSHA, err := verifyFinalFileIdentity(absolute, media.Size, expectedSHA, request.ID)
		if err != nil {
			return nil, NewTaskError("collision", false, err)
		}
		return &existingElement{task: task, file: media.File, path: finalPath, sha: verifiedSHA}, nil
	}

	element, err := newFileElement(task, media.File, r.tempRoot, r.outputRoot, media.Date)
	if err != nil {
		return nil, NewTaskError("filesystem", false, err)
	}
	return element, nil
}

func normalizePeer(peer string) string {
	return strings.TrimPrefix(strings.TrimSpace(peer), "@")
}
