module github.com/doublemo/nakama-plus/v3

go 1.24.5

replace github.com/doublemo/nakama-common v1.39.0 => ../nakama-common

require (
	github.com/blugelabs/bluge v0.2.2
	github.com/blugelabs/bluge_segment_api v0.2.0
	github.com/blugelabs/query_string v0.3.0
	github.com/dgraph-io/ristretto v0.2.0
	github.com/dgryski/dgoogauth v0.0.0-20190221195224-5a805980a5f3
	github.com/dop251/goja v0.0.0-20250309171923-bcd7cc6bf64c
	github.com/doublemo/nakama-common v1.39.0
	github.com/doublemo/nakama-kit v1.3.0
	github.com/eko/gocache/lib/v4 v4.2.0
	github.com/eko/gocache/store/ristretto/v4 v4.2.2
	github.com/felixge/httpsnoop v1.0.4
	github.com/gofrs/uuid/v5 v5.3.2
	github.com/golang-jwt/jwt/v5 v5.2.3
	github.com/gorilla/handlers v1.5.2
	github.com/gorilla/mux v1.8.1
	github.com/gorilla/websocket v1.5.3
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.27.1
	github.com/hashicorp/memberlist v0.5.3
	github.com/heroiclabs/sql-migrate v0.0.0-20241125131053-95a7949783b0
	github.com/jackc/pgerrcode v0.0.0-20240316143900-6e2875d9b438
	github.com/jackc/pgx/v5 v5.7.4
	github.com/klauspost/compress v1.18.0
	github.com/prometheus/client_golang v1.22.0
	github.com/prometheus/common v0.65.0
	github.com/spf13/cast v1.3.0
	github.com/stretchr/testify v1.10.0
	github.com/uber-go/tally/v4 v4.1.17
	go.etcd.io/etcd/client/v3 v3.5.14
	go.uber.org/atomic v1.11.0
	go.uber.org/zap v1.27.0
	golang.org/x/crypto v0.40.0
	golang.org/x/oauth2 v0.30.0
	google.golang.org/genproto/googleapis/api v0.0.0-20250603155806-513f23925822
	google.golang.org/grpc v1.74.2
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.5.1
	google.golang.org/protobuf v1.36.6
	gopkg.in/natefinch/lumberjack.v2 v2.2.1
	gopkg.in/yaml.v3 v3.0.1
)

require (
	cloud.google.com/go/compute/metadata v0.7.0 // indirect
	github.com/RoaringBitmap/roaring v1.9.4 // indirect
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/axiomhq/hyperloglog v0.0.0-20240507144631-af9851f82b27 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.13.0 // indirect
	github.com/blevesearch/go-porterstemmer v1.0.3 // indirect
	github.com/blevesearch/mmap-go v1.0.4 // indirect
	github.com/blevesearch/segment v0.9.1 // indirect
	github.com/blevesearch/snowballstem v0.9.0 // indirect
	github.com/blevesearch/vellum v1.0.10 // indirect
	github.com/blugelabs/ice v1.0.0 // indirect
	github.com/blugelabs/ice/v2 v2.0.1 // indirect
	github.com/caio/go-tdigest v3.1.0+incompatible // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/coreos/etcd v3.3.27+incompatible // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/coreos/pkg v0.0.0-20240122114842-bbd7aa9bf6fb // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-metro v0.0.0-20211217172704-adc40b04c140 // indirect
	github.com/dlclark/regexp2 v1.11.4 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/go-sourcemap/sourcemap v2.1.4+incompatible // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/mock v1.6.0 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/btree v0.0.0-20180813153112-4030bb1f1f0c // indirect
	github.com/google/pprof v0.0.0-20240528025155-186aa0362fba // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.0.0 // indirect
	github.com/hashicorp/go-metrics v0.5.4 // indirect
	github.com/hashicorp/go-msgpack/v2 v2.1.1 // indirect
	github.com/hashicorp/go-multierror v1.0.0 // indirect
	github.com/hashicorp/go-sockaddr v1.0.0 // indirect
	github.com/hashicorp/golang-lru v0.5.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/miekg/dns v1.1.26 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/twmb/murmur3 v1.1.8 // indirect
	github.com/vmihailenco/msgpack/v5 v5.4.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	go.etcd.io/etcd v3.3.27+incompatible // indirect
	go.etcd.io/etcd/api/v3 v3.5.14 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.14 // indirect
	go.uber.org/mock v0.4.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/exp v0.0.0-20240416160154-fe59bbe5cc7f // indirect
	golang.org/x/net v0.41.0 // indirect
	golang.org/x/sync v0.16.0 // indirect
	golang.org/x/sys v0.34.0 // indirect
	golang.org/x/text v0.27.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250603155806-513f23925822 // indirect
)
