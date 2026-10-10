// Copyright 2023 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package api

import (
	"net/http"

	"github.com/ethersphere/bee/v2/pkg/bigint"
	"github.com/ethersphere/bee/v2/pkg/jsonhttp"
	"github.com/ethersphere/bee/v2/pkg/tracing"
)

type redistributionStatusResponse struct {
	MinimumGasFunds           *bigint.BigInt `json:"minimumGasFunds"`
	HasSufficientFunds        bool           `json:"hasSufficientFunds"`
	IsFrozen                  bool           `json:"isFrozen"`
	IsFullySynced             bool           `json:"isFullySynced"`
	Phase                     string         `json:"phase"`
	Round                     uint64         `json:"round"`
	LastWonRound              uint64         `json:"lastWonRound"`
	LastPlayedRound           uint64         `json:"lastPlayedRound"`
	LastFrozenRound           uint64         `json:"lastFrozenRound"`
	LastSelectedRound         uint64         `json:"lastSelectedRound"`
	LastSampleDurationSeconds float64        `json:"lastSampleDurationSeconds"`
	Block                     uint64         `json:"block"`
	Reward                    *bigint.BigInt `json:"reward"`
	Fees                      *bigint.BigInt `json:"fees"`
	IsHealthy                 bool           `json:"isHealthy"`
	RevealPending             bool           `json:"revealPending"`
	SafeToRestartAfterBlock   uint64         `json:"safeToRestartAfterBlock"`
	StopWaitedForReveal       uint64         `json:"stopWaitedForReveal"`
	RevealMissedOnStop        uint64         `json:"revealMissedOnStop"`
	RevealAfterRestart        uint64         `json:"revealAfterRestart"`
}

func (s *Service) redistributionStatusHandler(w http.ResponseWriter, r *http.Request) {
	logger := tracing.NewLoggerWithTraceID(r.Context(), s.logger.WithName("get_redistributionstate").Build())

	if s.beeMode != FullMode {
		jsonhttp.BadRequest(w, errOperationSupportedOnlyInFullMode)
		return
	}

	status, err := s.redistributionAgent.Status()
	if err != nil {
		logger.Debug("get redistribution status", "overlay_address", s.overlay.String(), "error", err)
		logger.Error(nil, "get redistribution status")
		jsonhttp.InternalServerError(w, "failed to get redistribution status")
		return
	}

	minGasFunds, hasSufficientFunds, err := s.redistributionAgent.HasEnoughFundsToPlay(r.Context())
	if err != nil {
		logger.Debug("has enough funds to play", "overlay_address", s.overlay.String(), "error", err)
		logger.Error(nil, "has enough funds to play")
		jsonhttp.InternalServerError(w, "failed to calculate if node has enough funds to play")
		return
	}

	revealPending, safeToRestartAfterBlock := s.redistributionAgent.RevealPending()

	jsonhttp.OK(w, redistributionStatusResponse{
		MinimumGasFunds:           bigint.Wrap(minGasFunds),
		HasSufficientFunds:        hasSufficientFunds,
		IsFrozen:                  status.IsFrozen,
		IsFullySynced:             status.IsFullySynced,
		Phase:                     status.Phase.String(),
		LastWonRound:              status.LastWonRound,
		LastPlayedRound:           status.LastPlayedRound,
		LastFrozenRound:           status.LastFrozenRound,
		LastSelectedRound:         status.LastSelectedRound,
		LastSampleDurationSeconds: status.SampleDuration.Seconds(),
		Round:                     status.Round,
		Block:                     status.Block,
		Reward:                    bigint.Wrap(status.Reward),
		Fees:                      bigint.Wrap(status.Fees),
		IsHealthy:                 status.IsHealthy,
		RevealPending:             revealPending,
		SafeToRestartAfterBlock:   safeToRestartAfterBlock,
		StopWaitedForReveal:       status.StopWaitedForReveal,
		RevealMissedOnStop:        status.RevealMissedOnStop,
		RevealAfterRestart:        status.RevealAfterRestart,
	})
}
