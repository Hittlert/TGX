package tmedia

import (
	"github.com/gotd/td/tg"
)

type Media struct {
	InputFileLoc tg.InputFileLocationClass // mtproto file location of the media file
	Name         string                    // file name
	Size         int64                     // size in bytes
	DC           int                       // which DC the media is stored
	Date         int64                     // media creation(upload) timestamp
}

func ExtractMedia(m tg.MessageMediaClass) (*Media, bool) {
	switch m := m.(type) {
	case *tg.MessageMediaPhoto:
		return GetPhotoInfo(m)
	case *tg.MessageMediaDocument:
		return GetDocumentInfo(m)
	case *tg.MessageMediaInvoice:
		return GetExtendedMedia(m.ExtendedMedia)
	case *tg.MessageMediaPaidMedia:
		for _, pm := range m.ExtendedMedia {
			if res, ok := GetExtendedMedia(pm); ok {
				return res, true
			}
		}
	case *tg.MessageMediaWebPage:
		if wp, ok := m.Webpage.(*tg.WebPage); ok {
			if docClass, exists := wp.GetDocument(); exists {
				if doc, ok := docClass.(*tg.Document); ok {
					return GetDocumentInfo(&tg.MessageMediaDocument{Document: doc})
				}
			}
			if photoClass, exists := wp.GetPhoto(); exists {
				if photo, ok := photoClass.(*tg.Photo); ok {
					return GetPhotoInfo(&tg.MessageMediaPhoto{Photo: photo})
				}
			}
		}
	}
	return nil, false
}

func GetMedia(msg tg.MessageClass) (*Media, bool) {
	mm, ok := msg.(*tg.Message)
	if !ok {
		return nil, false
	}

	media, ok := mm.GetMedia()
	if !ok {
		return nil, false
	}

	return ExtractMedia(media)
}

func GetExtendedMedia(mm tg.MessageExtendedMediaClass) (*Media, bool) {
	m, ok := mm.(*tg.MessageExtendedMedia)
	if !ok {
		return nil, false
	}
	return ExtractMedia(m.Media)
}

func GetDocumentThumb(doc *tg.Document) (*Media, bool) {
	thumbs, exists := doc.GetThumbs()
	if !exists {
		return nil, false
	}

	photoSize := &tg.PhotoSize{}
	for _, t := range thumbs {
		if p, ok := t.(*tg.PhotoSize); ok {
			photoSize = p
			break
		}
	}

	if photoSize == nil {
		return nil, false
	}

	return &Media{
		InputFileLoc: &tg.InputDocumentFileLocation{
			ID:            doc.ID,
			AccessHash:    doc.AccessHash,
			FileReference: doc.FileReference,
			ThumbSize:     photoSize.Type,
		},
		Name: "thumb.jpg",
		Size: int64(photoSize.Size),
		DC:   doc.DCID,
		Date: int64(doc.Date),
	}, true
}
