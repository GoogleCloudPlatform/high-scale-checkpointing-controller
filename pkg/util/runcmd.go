// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"fmt"
	"os/exec"
	"strings"
)

const (
	// Error thrown by exec cmd.Run() when process spawned by cmd.Start() completes before cmd.Wait() is called (see - k/k issue #103753)
	errNoChildProcesses = "wait: no child processes"
)

// RunCommand wraps a k8s exec to deal with the no child process error. Same as exec.CombinedOutput.
// On error, the output is included so callers don't need to echo it again.
func RunCommand(cmd string, args ...string) ([]byte, error) {
	execCmd := exec.Command(cmd, args...)
	output, err := execCmd.CombinedOutput()
	if err != nil {
		if err.Error() == errNoChildProcesses {
			if execCmd.ProcessState.Success() {
				// If the process succeeded, this can be ignored, see k/k issue #103753
				return output, nil
			}
			// Get actual error
			err = &exec.ExitError{ProcessState: execCmd.ProcessState}
		}
		return output, fmt.Errorf("%s %s failed: %w; output: %s", cmd, strings.Join(args, " "), err, string(output))
	}
	return output, nil
}
