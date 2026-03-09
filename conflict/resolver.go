package conflict

type VersionedData struct {
	Value       string
	VectorClock VectorClock
	Timestamp   int64
}

type Resolver struct{}

func NewResolver() *Resolver {
	return &Resolver{}
}

func (r *Resolver) Resolve(versions []VersionedData) []VersionedData {
	if len(versions) <= 1 {
		return versions
	}

	var concurrent []VersionedData

	for i := 0; i < len(versions); i++ {
		isSuperseded := false

		for j := 0; j < len(versions); j++ {
			if i == j {
				continue
			}

			order := versions[i].VectorClock.Compare(versions[j].VectorClock)
			if order == Before {
				isSuperseded = true
				break
			}
			if order == Equal {
				if versions[i].Timestamp < versions[j].Timestamp {
					isSuperseded = true
					break
				}
				
				if versions[i].Timestamp == versions[j].Timestamp && i > j {
					isSuperseded = true
					break
				}
			}
		}

		if !isSuperseded {
			concurrent = append(concurrent, versions[i])
		}
	}

	return concurrent
}
