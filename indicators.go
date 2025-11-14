package main

import "math"

type atrTracker struct {
	period int
	value  float64
	ready  bool
}

func newATRTracker(period int) *atrTracker {
	if period <= 1 {
		period = 1
	}
	return &atrTracker{period: period}
}

func (a *atrTracker) Update(high, low, prevClose float64) float64 {
	tr := trueRange(high, low, prevClose)
	alpha := 2.0 / (float64(a.period) + 1)
	if !a.ready {
		a.value = tr
		a.ready = true
		return a.value
	}
	a.value = alpha*tr + (1-alpha)*a.value
	return a.value
}

func (a *atrTracker) Value() float64 { return a.value }

type adxTracker struct {
	period    int
	dxValues  []float64
	prevHigh  float64
	prevLow   float64
	prevClose float64
	seeded    bool
}

func newADXTracker(period int) *adxTracker {
	if period <= 1 {
		period = 14
	}
	return &adxTracker{period: period, dxValues: make([]float64, 0, period)}
}

func (a *adxTracker) Update(high, low, close float64) float64 {
	if !a.seeded {
		a.prevHigh = high
		a.prevLow = low
		a.prevClose = close
		a.seeded = true
		return 0
	}
	upMove := high - a.prevHigh
	downMove := a.prevLow - low
	dmPlus := 0.0
	dmMinus := 0.0
	if upMove > downMove && upMove > 0 {
		dmPlus = upMove
	}
	if downMove > upMove && downMove > 0 {
		dmMinus = downMove
	}
	tr := trueRange(high, low, a.prevClose)
	var diPlus, diMinus float64
	if tr > 0 {
		diPlus = 100 * dmPlus / tr
		diMinus = 100 * dmMinus / tr
	}
	denom := diPlus + diMinus
	dx := 0.0
	if denom > 0 {
		dx = math.Abs(diPlus-diMinus) / denom * 100
	}
	a.dxValues = append(a.dxValues, dx)
	if len(a.dxValues) > a.period {
		a.dxValues = a.dxValues[1:]
	}
	a.prevHigh = high
	a.prevLow = low
	a.prevClose = close
	return a.Value()
}

func (a *adxTracker) Value() float64 {
	if len(a.dxValues) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range a.dxValues {
		sum += v
	}
	return sum / float64(len(a.dxValues))
}
