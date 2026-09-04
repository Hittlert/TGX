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
	var bestPixels int64
	found := false

	for _, size := range sizes {
		var (
			tp    string
			w, h  int
			sz    int
			valid bool
		)

		switch s := size.(type) {
		case *tg.PhotoSize:
			tp = s.Type
			w = s.W
			h = s.H
			sz = s.Size
			valid = true
		case *tg.PhotoSizeProgressive:
			tp = s.Type
			w = s.W
			h = s.H
			if len(s.Sizes) > 0 {
				sz = s.Sizes[len(s.Sizes)-1]
			}
			valid = true
		case *tg.PhotoCachedSize:
			tp = s.Type
			w = s.W
			h = s.H
			sz = len(s.Bytes)
			valid = true
		}

		if !valid {
			continue
		}

		pixels := int64(w) * int64(h)
		if !found {
			bestType = tp
			bestSize = sz
			bestPixels = pixels
			found = true
			continue
		}

		// Prefer higher resolution (pixel count), fallback to larger file size if resolution matches
		if pixels > bestPixels || (pixels == bestPixels && sz > bestSize) {
			bestType = tp
			bestSize = sz
			bestPixels = pixels
		}
	}

	return bestType, bestSize, found
}
