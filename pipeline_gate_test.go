package pixelstream

// setEncodeGate installs a per-frame gate used only by tests: the encoder
// blocks on the gate before encoding each frame, giving deterministic control
// over drain pacing and hence over the queue-depth signal seen by adaptation.
func (p *Pipeline) setEncodeGate(g <-chan struct{}) { p.enc.gateBeforeFrame = g }
