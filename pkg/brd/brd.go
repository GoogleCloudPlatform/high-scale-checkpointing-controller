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

package brd

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/pkg/util"
)

const (
	blockdevCmd     = "/usr/sbin/blockdev"
	blockdevGetSize = "getsize64"
	modprobeCmd     = "/usr/sbin/modprobe"
	brdPathTemplate = "/dev/ram%d"
)

// StartBrd starts the block ram device system with devices of the specified size in KiB and count.
// It verifies the devices exist as specified. It's safe to call this multiple times with the same
// parameters (but once created, brd parameters can't be changed).
func StartBrd(kb, ndev int) error {
	kbStr := fmt.Sprintf("rd_size=%d", kb)
	ndevStr := fmt.Sprintf("rd_nr=%d", ndev)
	if output, err := util.RunCommand(modprobeCmd, "brd", kbStr, ndevStr); err != nil {
		return fmt.Errorf("failed to run modprobe (%w): %s", err, output)
	}
	for i := 0; i < ndev; i++ {
		dev, err := RamDevice(i)
		if err != nil {
			return fmt.Errorf("could not verify %s: %w", dev, err)
		}
		sz, err := blockdevInt64(dev, blockdevGetSize)
		if err != nil {
			return fmt.Errorf("Could not verify %s: %w", dev, err)
		}
		if sz/1024 != int64(kb) {
			return fmt.Errorf("%s has size %d but expected %d", dev, sz/1024, kb)
		}
	}
	return nil
}

func RamDevice(idx int) (string, error) {
	dev := fmt.Sprintf(brdPathTemplate, idx)
	if _, err := os.Stat(dev); os.IsNotExist(err) {
		return "", fmt.Errorf("brd device %s does not exist", dev)
	}
	return dev, nil
}

func blockdevInt64(device, cmd string) (int64, error) {
	output, err := util.RunCommand(blockdevCmd, "--"+cmd, device)
	if err != nil {
		return 0, err
	}
	outStr := strings.TrimSpace(string(output))
	return strconv.ParseInt(outStr, 10, 64)
}
