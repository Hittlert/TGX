package tmedia

import (
	"strconv"

	"github.com/gotd/td/tg"
)

func GetPhotoInfo(photo *tg.MessageMediaPhoto) (*Media, bool) {
	p, ok := photo.Photo.(*tg.Photo)
	if !ok {
		return nil, false
	}

	tp, size, ok := GetPhotoSize(p.Sizes)
	if !ok {
		return nil, false
	}
	return &Media{
		InputFileLoc: &tg.InputPhotoFileLocation{
			ID:            p.ID,
			AccessHash:    p.AccessHash,
			FileReference: p.FileReference,
			ThumbSize:     tp,
		},
		// Telegram photo is compressed, and extension is always jpg.
		Name: strconv.FormatInt(p.ID, 10) + ".jpg", // unique name
		Size: int64(size),
		DC:   p.DCID,
		Date: int64(p.Date),
	}, true
}

func GetPhotoSize(sizes []tg.PhotoSizeClass) (string, int, bool) {
	var bestType string
	var bestSize int
	found := false

	for _, size := range sizes {
		switch s := size.(type) {
		case *tg.PhotoSize:
			if s.Size > bestSize || !found {
				bestType = s.Type
				bestSize = s.Size
				found = true
			}
		case *tg.PhotoSizeProgressive:
			maxSize := 0
			if len(s.Sizes) > 0 {
				maxSize = s.Sizes[len(s.Sizes)-1]
			}
			if maxSize > bestSize || !found {
				bestType = s.Type
				bestSize = maxSize
				found = true
			}
		}
	}

	return bestType, bestSize, found
}
