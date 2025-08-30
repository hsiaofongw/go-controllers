package utils

type RevChangeLog struct {
	Generation         string `json:"generation"`
	ResourceVersion    string `json:"resourceVersion"`
	ObservedGeneration string `json:"observedGeneration"`
}
