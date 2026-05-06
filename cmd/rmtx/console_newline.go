package main

import "io"

type crlfLineFeedWriter struct {
	w      io.Writer
	lastCR bool
}

func (w *crlfLineFeedWriter) Write(p []byte) (int, error) {
	out := make([]byte, 0, len(p)+1)
	for _, b := range p {
		if b == '\n' && !w.lastCR {
			out = append(out, '\r', '\n')
			w.lastCR = false

			continue
		}

		out = append(out, b)
		w.lastCR = b == '\r'
	}

	if _, err := w.w.Write(out); err != nil {
		return 0, err
	}

	return len(p), nil
}
