package protocol

import (
	"context"
	"time"
)

func StartHeartbeat(
	ctx context.Context,
	conn *Conn,
	interval time.Duration,
	onError func(error),
) func() {
	if interval <= 0 || conn == nil {
		return func() {}
	}

	ctx, cancel := context.WithCancel(ctx)

	go func() {
		timer := time.NewTimer(0)
		defer timer.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				if err := conn.WriteJSON(MsgHeartbeat, nil); err != nil {
					if onError != nil {
						onError(err)
					}

					return
				}

				timer.Reset(interval)
			}
		}
	}()

	return cancel
}
