// Command fixturegen regenerates the Layer 16 search fixtures under
// go/testdata/golden/search from the Go implementations. It is deterministic: running it
// twice yields byte-identical files. Expected values were checked by hand before being pinned
// (see the *_test.go files that cite the fixtures); regenerate only after a deliberate
// contract change, and re-verify the hand-derived cases.
//
//	cd go && go run ./kdb/index/internal/fixturegen -out testdata/golden/search
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/index"
	"github.com/limidus/kdb/go/kdb/index/analyzer"
	"github.com/limidus/kdb/go/kdb/index/fulltext"
	"github.com/limidus/kdb/go/kdb/index/fusion"
	"github.com/limidus/kdb/go/kdb/index/vector"
)

func main() {
	out := flag.String("out", "testdata/golden/search", "output directory")
	flag.Parse()
	if err := os.MkdirAll(*out, 0o755); err != nil {
		fail(err)
	}
	write(*out, "analyzer_vectors.json", analyzerVectors())
	write(*out, "porter_vectors.txt", porterVectors())
	write(*out, "bm25_corpus.json", bm25Corpus())
	write(*out, "vector_corpus.json", vectorCorpus())
	write(*out, "fusion_cases.json", fusionCases())
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func write(dir, name string, content []byte) {
	if err := os.WriteFile(filepath.Join(dir, name), content, 0o644); err != nil {
		fail(err)
	}
}

func marshal(v any) []byte {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fail(err)
	}
	return append(b, '\n')
}

// num renders a float32 as the shortest decimal that parses back to exactly that float32.
func num(f float32) json.Number { return json.Number(strconv.FormatFloat(float64(f), 'g', -1, 32)) }

func fixedUUID(n int) codec.UUID {
	id, err := codec.UUIDFromString(fmt.Sprintf("00000000-0000-4000-8000-%012d", n))
	if err != nil {
		fail(err)
	}
	return id
}

func rankedJSON(results []index.RankedResult) [][]any {
	out := make([][]any, 0, len(results))
	for _, r := range results {
		out = append(out, []any{r.DocID.String(), num(r.Score)})
	}
	return out
}

// ---- analyzer_vectors.json -------------------------------------------------------------

func analyzerVectors() []byte {
	texts := []string{
		"The quick brown fox jumps over the lazy dog",
		"Deploy staging environment",
		"Users cannot log in after the password reset flow changed",
		"  multiple   spaces\tand\nnewlines ",
		"Hyphenated-words and under_scores split",
		"version 2.4 released 2024-09-05",
		"ALL CAPS SHOUTING",
		"Ünïcödé naïve café résumé",
		"日本語 テキスト and English words",
		"",
		"the and of to",
		"mp3s utf8s x86",
		strings.Repeat("a", 65) + " short " + strings.Repeat("b", 64),
		"email@example.com sent at 10:30am",
		"it's not a bug, it's a feature!",
		"Tags: deploy, ops; staging/production",
		"the deploy of the staging",
		"Relational conditional rational operators",
		"hopping hoped hopes hopefully",
		"CamelCaseWords are one token",
	}
	type vec struct {
		Text   string   `json:"text"`
		Tokens []string `json:"tokens"`
	}
	out := make([]vec, 0, len(texts))
	for _, t := range texts {
		toks := analyzer.Terms(t)
		if toks == nil {
			toks = []string{}
		}
		out = append(out, vec{Text: t, Tokens: toks})
	}
	return marshal(out)
}

// ---- porter_vectors.txt ----------------------------------------------------------------

func porterVectors() []byte {
	words := strings.Fields(`
caresses ponies ties caress cats feed agreed plastered motoring sing conflated troubled sized
hopping tanned falling hissing fizzed failing filing happy sky relational conditional rational
valenci hesitanci digitizer conformabli radicalli differentli vileli analogousli vietnamization
predication operator feudalism decisiveness hopefulness callousness formaliti sensitiviti
sensibiliti triplicate formative formalize electriciti electrical hopeful goodness revival
allowance inference airliner gyroscopic adjustable defensible irritant replacement adjustment
dependent adoption homologou communism activate angulariti homologous effective bowdlerize
probate rate cease controll roll
generalization generalize generalizations organization organizational organizations
national nationalize nationalization international internationalization
running runner runs ran walking walked walks talked talking talks
studies studying studied study flies flying fly flew
happiness happily unhappy sadness sadly
beautiful beautifully beauty
computer computers computing computed computation computational
engineer engineering engineered engineers
deploying deployed deployment deployments deploys deploy
staging staged stages stage
database databases
migration migrations migrate migrated migrating
production productive productivity produce produced producing
configuration configure configured configuring configurations
authentication authenticate authenticated authorization authorize authorized
performance performing performed performs
security secure secured securing securities
availability available
reliability reliable reliably
scalability scalable scaling scaled
optimization optimize optimized optimizing
visualization visualize visualized
documentation document documented documenting documents
implementation implement implemented implementing implements
integration integrate integrated integrating
notification notify notified notifying notifications
validation validate validated validating
serialization serialize serialized
initialization initialize initialized
synchronization synchronize synchronized
recommendation recommend recommended
investigation investigate investigated investigating
transaction transactions transactional
connection connections connected connecting connectivity
exception exceptional exceptions
condition conditions conditioned
argument arguments argued arguing
agreement agreements agreeing
management manage managed manager managers managing
development develop developed developer developers developing
environment environments environmental
government governing governed
requirement requirements required requiring
statement statements stated stating
element elements elemental
movement moved moving moves
department departments departed
knowledge knowing known knows
electricity electric electrician
history historical historic
policy policies political politics
enemy enemies
city cities
company companies
family families
memory memories
category categories
priority priorities
query queries queried querying
entry entries
summary summaries summarise summarised
retry retries retried
apply applies applied applying
supply supplies supplied
try tries tried trying
dry dries dried drying
ally allies allied
reply replies replied
early late later latest
faster fastest fast
bigger biggest big
smaller smallest small
larger largest large
easier easiest easy easily
heavier heaviest heavy
noisier noisiest noisy
happier happiest
crying cried cries cry
dying died dies die
lying lied lies lie
tying tied
being been is was are were am
having has had have
doing does did done do
going goes went gone go
seeing sees saw seen see
making makes made make
taking takes took taken take
coming comes came come
giving gives gave given give
getting gets got gotten get
sitting sits sat sit
setting sets set
hitting hits hit
cutting cuts cut
putting puts put
letting lets let
stopping stopped stops stop
dropping dropped drops drop
shipping shipped ships ship
planning planned plans plan
beginning began begun begin
winning won wins win
sinning sinned
fitting fitted fits fit
matter matters mattered
better best good
worse worst bad
mice mouse
children child
women woman men man
feet foot teeth tooth geese goose
sheep fish deer
alumni alumnus cacti cactus fungi fungus
analysis analyses analyse analysed analysing
crisis crises
thesis theses
basis bases
axis axes
criterion criteria
phenomenon phenomena
datum data
medium media
index indices indexes indexed indexing
matrix matrices
vertex vertices vertexes
appendix appendices
address addresses addressed addressing
process processes processed processing processor
access accessed accessing accessible
success successes successful successfully
business businesses
witness witnesses witnessed
class classes classified classification
pass passes passed passing
glass glasses
mass masses massive
grass
boss bosses
loss losses lost lose losing
cross crosses crossed crossing
dress dresses dressed
press presses pressed pressing pressure
stress stresses stressed stressful
express expresses expressed expression
progress progressed progression progressive
regress regression
aggress aggressive aggression
possess possesses possessed possession
assess assesses assessed assessment
guess guesses guessed guessing
bless blessed blessing
confess confessed confession
`)
	seen := make(map[string]struct{}, len(words))
	var lines []string
	for _, w := range words {
		w = strings.ToLower(w)
		if _, dup := seen[w]; dup {
			continue
		}
		seen[w] = struct{}{}
		lines = append(lines, w+" "+analyzer.Stem(w))
	}
	return []byte(strings.Join(lines, "\n") + "\n")
}

// ---- bm25_corpus.json ------------------------------------------------------------------

type corpusDoc struct {
	ID   string `json:"id"`
	JSON string `json:"json"`
}

type corpusQuery struct {
	Query    string  `json:"query"`
	Expected [][]any `json:"expected"`
}

type corpusField struct {
	Path   string  `json:"path"`
	Weight float64 `json:"weight"`
}

func bm25Docs() []corpusDoc {
	raw := []string{
		`{"title":"Deploy staging environment","description":"Deploy the new build to the staging cluster and verify health checks","tags":["deploy","ops"]}`,
		`{"title":"Fix login bug","description":"Users cannot log in after the password reset flow changed","tags":["bug","auth"]}`,
		`{"title":"Write release notes","description":"Summarise the changes shipped in the deploy of version 2.4","tags":["docs"]}`,
		`{"title":"Staging database migration","description":"Run the migration scripts against staging before production","tags":["database","ops","staging"]}`,
		`{"title":"Production deploy checklist","description":"Checklist for deploying to production: backups, alerts, rollback plan","tags":["deploy","staging","production"]}`,
		`{"title":"Rotate API keys","description":"Rotate the third-party API keys and update the secrets store","tags":["security","ops"]}`,
		`{"title":"Investigate slow queries","description":"The reporting dashboard times out; profile the database queries","tags":["database","performance"]}`,
		`{"title":"Onboard new engineer","description":"Set up accounts, laptop and access for the new hire"}`,
		`{"title":"Deploy deploy deploy","description":"Repeated word test document for term frequency saturation","tags":[]}`,
		`{"title":"Verify health checks","description":"The staging health checks flap after deploy; investigate the load balancer","tags":["ops","staging","deploy"]}`,
		`{"title":"Password reset emails","description":"Reset emails are delayed; check the mail queue and retry policy","tags":["auth","email"]}`,
		`{"title":"Empty tags document","description":"This document has tags that are not strings","tags":[1,2,true]}`,
	}
	docs := make([]corpusDoc, len(raw))
	for i, j := range raw {
		docs[i] = corpusDoc{ID: fixedUUID(i + 1).String(), JSON: j}
	}
	return docs
}

func bm25Descriptor() index.Descriptor {
	return index.Descriptor{
		IndexID:     fixedUUID(9001),
		NamespaceID: "fixtures/tasks",
		FieldName:   "title",
		Fields:      []string{"title", "description", "tags"},
		Type:        index.IndexTypeFullText,
		Options:     map[string]string{"index_name": "tasks_text", "weights": "title=3,tags=2"},
	}
}

func bm25Corpus() []byte {
	d, err := dag.NewInMemoryCommitDag("fixtures/tasks")
	if err != nil {
		fail(err)
	}
	head, _ := d.Head()
	store, err := fulltext.NewFullTextStore(bm25Descriptor(), d, fulltext.Options{})
	if err != nil {
		fail(err)
	}
	docs := bm25Docs()
	for _, doc := range docs {
		id, _ := codec.UUIDFromString(doc.ID)
		if err := store.PutDocument(id, head, doc.JSON); err != nil {
			fail(err)
		}
	}
	queries := []string{
		"database",
		"deploy",
		"staging deploy",
		`"deploy staging"`,
		"the and of to",
		"health checks",
		"password reset flow",
		"Deploying STAGING!",
		"xyzzy plugh",
		`"health checks" staging`,
		"ops",
		`"the deploy"`,
	}
	out := struct {
		Index struct {
			Fields []corpusField `json:"fields"`
		} `json:"index"`
		Documents []corpusDoc   `json:"documents"`
		Queries   []corpusQuery `json:"queries"`
	}{Documents: docs}
	out.Index.Fields = []corpusField{{"title", 3}, {"description", 1}, {"tags", 2}}
	for _, q := range queries {
		res, err := store.Search(q, nil, 0)
		if err != nil {
			fail(err)
		}
		out.Queries = append(out.Queries, corpusQuery{Query: q, Expected: rankedJSON(res)})
	}
	return marshal(out)
}

// ---- vector_corpus.json ----------------------------------------------------------------

func vectorCorpus() []byte {
	const dims = 8
	rng := rand.New(rand.NewSource(1601))
	round := func(f float64) float32 { return float32(float64(int(f*1000)) / 1000) }
	type vdoc struct {
		ID     string        `json:"id"`
		Vector []json.Number `json:"vector"`
	}
	type vquery struct {
		Metric   string        `json:"metric"`
		Vector   []json.Number `json:"vector"`
		K        int           `json:"k"`
		Expected [][]any       `json:"expected"`
	}
	nums := func(v []float32) []json.Number {
		out := make([]json.Number, len(v))
		for i, f := range v {
			out[i] = num(f)
		}
		return out
	}
	vectors := make([][]float32, 20)
	for i := range vectors {
		v := make([]float32, dims)
		for j := range v {
			v[j] = round(rng.Float64()*2 - 1)
		}
		vectors[i] = v
	}
	// Document 20 is the zero vector: cosine against it must be 0, not NaN.
	vectors[19] = make([]float32, dims)
	// Documents 18 and 19 are the same vector, so they tie on every metric and must be
	// ordered by id.
	copy(vectors[18], vectors[17])

	d, err := dag.NewInMemoryCommitDag("fixtures/vectors")
	if err != nil {
		fail(err)
	}
	head, _ := d.Head()
	docs := make([]vdoc, 0, len(vectors))
	queries := []vquery{}
	queryVectors := make([][]float32, 3)
	for i := range queryVectors {
		v := make([]float32, dims)
		for j := range v {
			v[j] = round(rng.Float64()*2 - 1)
		}
		queryVectors[i] = v
	}
	queryVectors[2] = append([]float32(nil), vectors[17]...) // exact hit on the tied pair
	for _, metric := range []vector.Metric{vector.Cosine, vector.L2, vector.InnerProduct} {
		desc := index.Descriptor{
			IndexID: fixedUUID(9002), NamespaceID: "fixtures/vectors", FieldName: "embedding",
			Fields: []string{"embedding"}, Type: index.IndexTypeVector,
			Options: map[string]string{"dimensions": strconv.Itoa(dims), "metric": metric.String()},
		}
		store, err := vector.NewVectorStore(desc, d, vector.Options{})
		if err != nil {
			fail(err)
		}
		for i, v := range vectors {
			if err := store.Put(index.Entry{DocID: fixedUUID(i + 1), Key: index.NewVectorKey(v), CommitHash: head}); err != nil {
				fail(err)
			}
		}
		for qi, qv := range queryVectors {
			k := 5
			if qi == 1 {
				k = 20
			}
			res, err := store.ExactNearestNeighbours(qv, k, nil)
			if err != nil {
				fail(err)
			}
			queries = append(queries, vquery{Metric: metric.String(), Vector: nums(qv), K: k, Expected: rankedJSON(res)})
		}
	}
	for i, v := range vectors {
		docs = append(docs, vdoc{ID: fixedUUID(i + 1).String(), Vector: nums(v)})
	}
	out := struct {
		Dimensions int      `json:"dimensions"`
		Documents  []vdoc   `json:"documents"`
		Queries    []vquery `json:"queries"`
	}{Dimensions: dims, Documents: docs, Queries: queries}
	return marshal(out)
}

// ---- fusion_cases.json -----------------------------------------------------------------

type fusionArmJSON struct {
	Weight   float64  `json:"weight"`
	Depth    int      `json:"depth"`
	MinScore *float64 `json:"minScore"`
	Results  [][]any  `json:"results"`
}

type fusionCaseJSON struct {
	Name     string          `json:"name"`
	Mode     string          `json:"mode"`
	Limit    int             `json:"limit"`
	Arms     []fusionArmJSON `json:"arms"`
	Expected [][]any         `json:"expected"`
}

func fusionCases() []byte {
	type r = index.RankedResult
	id := fixedUUID
	list := func(pairs ...any) []r {
		var out []r
		for i := 0; i < len(pairs); i += 2 {
			out = append(out, r{DocID: id(pairs[i].(int)), Score: float32(pairs[i+1].(float64))})
		}
		return out
	}
	arm := func(results []r, weight float64, depth int, minScore *float64) fusion.Arm {
		a := fusion.Arm{Results: results, Weight: weight, Depth: depth}
		if minScore != nil {
			a.HasMinScore = true
			a.MinScore = float32(*minScore)
		}
		return a
	}
	f := func(v float64) *float64 { return &v }
	type tc struct {
		name  string
		mode  fusion.Mode
		limit int
		arms  []fusion.Arm
	}
	A := list(1, 1.0, 2, 0.5, 3, 0.25)
	Bm := list(2, 0.9, 4, 0.8, 1, 0.1)
	cases := []tc{
		{"rrf_two_arms_overlap", fusion.ModeRRF, 0, []fusion.Arm{arm(A, 1, 0, nil), arm(Bm, 1, 0, nil)}},
		{"rrf_disjoint_arms", fusion.ModeRRF, 0, []fusion.Arm{arm(list(1, 3.0, 2, 2.0), 1, 0, nil), arm(list(3, 0.9, 4, 0.8), 1, 0, nil)}},
		{"rrf_weights", fusion.ModeRRF, 0, []fusion.Arm{arm(A, 2, 0, nil), arm(Bm, 0.5, 0, nil)}},
		{"rrf_depth_truncates_after_filter", fusion.ModeRRF, 0, []fusion.Arm{arm(A, 1, 2, nil), arm(Bm, 1, 1, nil)}},
		{"rrf_min_score_drops_low_results", fusion.ModeRRF, 0, []fusion.Arm{arm(A, 1, 0, f(0.5)), arm(Bm, 1, 0, f(0.5))}},
		{"rrf_limit", fusion.ModeRRF, 2, []fusion.Arm{arm(A, 1, 0, nil), arm(Bm, 1, 0, nil)}},
		{"rrf_exact_tie_orders_by_doc_id", fusion.ModeRRF, 0, []fusion.Arm{arm(list(5, 1.0, 6, 0.5), 1, 0, nil), arm(list(6, 1.0, 5, 0.5), 1, 0, nil)}},
		{"rrf_single_arm", fusion.ModeRRF, 0, []fusion.Arm{arm(A, 1, 0, nil)}},
		{"rrf_empty_arm_contributes_nothing", fusion.ModeRRF, 0, []fusion.Arm{arm(A, 1, 0, nil), arm(nil, 1, 0, nil)}},
		{"weighted_two_arms_overlap", fusion.ModeWeightedSum, 0, []fusion.Arm{arm(A, 1, 0, nil), arm(Bm, 1, 0, nil)}},
		{"weighted_all_equal_scores_normalise_to_one", fusion.ModeWeightedSum, 0, []fusion.Arm{arm(list(1, 0.7, 2, 0.7, 3, 0.7), 1, 0, nil), arm(list(2, 0.2), 1, 0, nil)}},
		{"weighted_min_score_then_depth", fusion.ModeWeightedSum, 0, []fusion.Arm{arm(list(1, 10.0, 2, 8.0, 3, 6.0, 4, 1.0), 1, 2, f(5.0)), arm(Bm, 1, 0, nil)}},
		{"weighted_weights_and_limit", fusion.ModeWeightedSum, 3, []fusion.Arm{arm(A, 0.25, 0, nil), arm(Bm, 3, 0, nil)}},
		{"weighted_disjoint_arms", fusion.ModeWeightedSum, 0, []fusion.Arm{arm(list(1, 3.0, 2, 2.0), 1, 0, nil), arm(list(3, 0.9, 4, 0.8), 2, 0, nil)}},
		{"weighted_exact_tie_orders_by_doc_id", fusion.ModeWeightedSum, 0, []fusion.Arm{arm(list(8, 1.0, 7, 0.0), 1, 0, nil), arm(list(7, 1.0, 8, 0.0), 1, 0, nil)}},
		{"weighted_negative_scores_min_max", fusion.ModeWeightedSum, 0, []fusion.Arm{arm(list(1, 0.5, 2, -0.25, 3, -1.0), 1, 0, nil)}},
	}
	out := struct {
		Cases []fusionCaseJSON `json:"cases"`
	}{}
	for _, c := range cases {
		cj := fusionCaseJSON{Name: c.name, Limit: c.limit, Mode: "rrf"}
		if c.mode == fusion.ModeWeightedSum {
			cj.Mode = "weighted"
		}
		for _, a := range c.arms {
			aj := fusionArmJSON{Weight: a.Weight, Depth: a.Depth, Results: rankedJSON(a.Results)}
			if aj.Results == nil {
				aj.Results = [][]any{}
			}
			if a.HasMinScore {
				aj.MinScore = f(float64(a.MinScore))
			}
			cj.Arms = append(cj.Arms, aj)
		}
		cj.Expected = rankedJSON(fusion.Fuse(c.arms, c.mode, c.limit))
		out.Cases = append(out.Cases, cj)
	}
	return marshal(out)
}
