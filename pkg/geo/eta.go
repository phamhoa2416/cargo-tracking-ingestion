package geo

func CalculateETA(distanceKm, speedKmh float64) int {
	if speedKmh <= 0 {
		return 0
	}
	hours := distanceKm / speedKmh
	return int(hours * 60)
}
