package tmedia

import (
	"strconv"
	"strings"

	"github.com/gabriel-vasile/mimetype"
	"github.com/gotd/td/tg"
)

func GetDocumentInfo(doc *tg.MessageMediaDocument) (*Media, bool) {
	d, ok := doc.Document.(*tg.Document)
	if !ok {
		return nil, false
	}

	return &Media{
		InputFileLoc: &tg.InputDocumentFileLocation{
			ID:            d.ID,
			AccessHash:    d.AccessHash,
			FileReference: d.FileReference,
		},
		Name: GetDocumentName(d),
		Size: d.Size,
		DC:   d.DCID,
		Date: int64(d.Date),
	}, true
}

func GetDocumentName(doc *tg.Document) string {
	for _, attr := range doc.Attributes {
		name, ok := attr.(*tg.DocumentAttributeFilename)
		if ok && name.FileName != "" {
			return name.FileName
		}
	}

	mime := mimetype.Lookup(doc.MimeType)
	ext := ""
	if mime != nil {
		ext = mime.Extension()
	}
	if ext == "" || ext == ".unknown" {
		for _, attr := range doc.Attributes {
			switch attr.(type) {
			case *tg.DocumentAttributeVideo:
				ext = ".mp4"
			case *tg.DocumentAttributeAudio:
				ext = ".mp3"
			case *tg.DocumentAttributeAnimated:
				ext = ".mp4"
			case *tg.DocumentAttributeSticker:
				ext = ".webp"
			}
		}
	}
	if ext == "" || ext == ".unknown" {
		if strings.HasPrefix(doc.MimeType, "video/") {
			ext = ".mp4"
		} else if strings.HasPrefix(doc.MimeType, "image/") {
			ext = ".jpg"
		} else if strings.HasPrefix(doc.MimeType, "audio/") {
			ext = ".mp3"
		} else {
			ext = ".bin"
		}
	}
	return strconv.FormatInt(doc.ID, 10) + ext
}
