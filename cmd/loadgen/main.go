// Command loadgen will drive the Black Friday experiment with open-loop, seeded
// load (design §17). It arrives in phase 5; until then it only says so.
package main

import (
	"fmt"
	"os"
)

func main() {
	_, _ = fmt.Fprintln(os.Stderr, "loadgen: not built yet (phase 5)")
	os.Exit(2)
}
