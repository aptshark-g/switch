package token

// Counter provides token estimation using a fast heuristic (chars / 4).
// When tiktoken-go is available it will use exact token counting.
type Counter struct {
	charsPerToken int
}

func NewCounter() *Counter {
	return &Counter{charsPerToken: 4}
}

// Count estimates the number of tokens in the given text.
func (c *Counter) Count(text string) int {
	if text == "" {
		return 0
	}
	n := len(text) / c.charsPerToken
	if n < 1 {
		return 1
	}
	return n
}

// CountMessages estimates total tokens across a list of message strings.
func (c *Counter) CountMessages(messages []string) int {
	total := 0
	for _, m := range messages {
		total += c.Count(m)
	}
	return total
}
