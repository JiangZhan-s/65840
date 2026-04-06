package shardcfg

import (
	"encoding/json"
	"hash/fnv"
	"log"
	"runtime/debug"
	"slices"
	"testing"

	tester "6.5840/tester1"
)

type Tshid int
type Tnum int

const (
	NShards  = 12 // The number of shards.
	NumFirst = Tnum(1)
)

const (
	Gid1 = tester.Tgid(1)
)

// 一个 key 属于哪个 shard？
// 请使用这个函数，
// 并且不要修改它。
func Key2Shard(key string) Tshid {
	h := fnv.New32a()
	h.Write([]byte(key))
	shard := Tshid(Tshid(h.Sum32()) % NShards)
	return shard
}

// 一个配置：将各个 shard 分配到不同 group。
// 请不要修改这里。
type ShardConfig struct {
	Num    Tnum                     // config number
	Shards [NShards]tester.Tgid     // shard -> gid
	Groups map[tester.Tgid][]string // gid -> servers[]
}

func MakeShardConfig() *ShardConfig {
	c := &ShardConfig{
		Groups: make(map[tester.Tgid][]string),
	}
	return c
}

func (cfg *ShardConfig) String() string {
	b, err := json.Marshal(cfg)
	if err != nil {
		log.Fatalf("Unmarshall err %v", err)
	}
	return string(b)
}

func FromString(s string) *ShardConfig {
	scfg := &ShardConfig{}
	if err := json.Unmarshal([]byte(s), scfg); err != nil {
		log.Fatalf("Unmarshall err %v", err)
	}
	return scfg
}

func (cfg *ShardConfig) Copy() *ShardConfig {
	c := MakeShardConfig()
	c.Num = cfg.Num
	c.Shards = cfg.Shards
	for k, srvs := range cfg.Groups {
		s := make([]string, len(srvs))
		copy(s, srvs)
		c.Groups[k] = s
	}
	return c
}

// 最多 shard 的 group、其数量、最少 shard 的 group、其数量
func analyze(c *ShardConfig) (tester.Tgid, int, tester.Tgid, int) {
	counts := map[tester.Tgid]int{}
	for _, g := range c.Shards {
		counts[g] += 1
	}

	mn := -1
	var mg tester.Tgid = -1
	ln := 257
	var lg tester.Tgid = -1
	// 强制确定性顺序，
	// Go 中 map 的迭代顺序是随机的
	groups := make([]tester.Tgid, len(c.Groups))
	i := 0
	for k := range c.Groups {
		groups[i] = k
		i++
	}
	slices.Sort(groups)
	for _, g := range groups {
		if counts[g] < ln {
			ln = counts[g]
			lg = g
		}
		if counts[g] > mn {
			mn = counts[g]
			mg = g
		}
	}

	return mg, mn, lg, ln
}

// 返回已分配 shard 数量最少的 group 的 GID。
func least(c *ShardConfig) tester.Tgid {
	_, _, lg, _ := analyze(c)
	return lg
}

// 平衡 shard 到各个 group 的分配。
// 会直接修改 c。
func (c *ShardConfig) Rebalance() {
	// 如果没有任何 group，则取消所有 shard 的分配
	if len(c.Groups) < 1 {
		for s, _ := range c.Shards {
			c.Shards[s] = 0
		}
		return
	}

	// 为所有未分配的 shard 分配 group
	for s, g := range c.Shards {
		_, ok := c.Groups[g]
		if ok == false {
			lg := least(c)
			c.Shards[s] = lg
		}
	}

	// 从负载最重的 group 向负载最轻的 group 迁移 shard
	for {
		mg, mn, lg, ln := analyze(c)
		if mn < ln+2 {
			break
		}
		// 从 mg 向 lg 迁移 1 个 shard
		for s, g := range c.Shards {
			if g == mg {
				c.Shards[s] = lg
				break
			}
		}
	}
}

// 输入是一个map，key是新加入的group的GID，value是该group中服务器的列表。
// 返回值是一个布尔值，表示是否成功加入。
func (cfg *ShardConfig) Join(servers map[tester.Tgid][]string) bool {
	changed := false
	// 遍历输入的map，检查每个新加入的group是否已经存在于当前配置中，以及是否有服务器重复。
	for gid, servers := range servers {
		_, ok := cfg.Groups[gid]
		if ok {
			log.Printf("re-Join %v", gid)
			return false
		}
		// 检查新加入的服务器是否与当前配置中的服务器重复
		for xgid, xservers := range cfg.Groups {
			for _, s1 := range xservers {
				for _, s2 := range servers {
					if s1 == s2 {
						log.Fatalf("Join(%v) puts server %v in groups %v and %v", gid, s1, xgid, gid)
					}
				}
			}
		}
		// 新加入的 GID
		// 修改 cfg 以反映 Join()
		cfg.Groups[gid] = servers
		changed = true
	}
	// 这是防止输入的map为空，或者所有的GID都已经存在于当前配置中，这两种情况都会导致没有任何变化。
	if changed == false {
		log.Fatalf("Join but no change")
	}
	cfg.Num += 1
	return true
}

// 输入是一个包含要离开的group的GID的列表。返回值是一个布尔值，表示是否成功离开。
func (cfg *ShardConfig) Leave(gids []tester.Tgid) bool {
	changed := false
	for _, gid := range gids {
		_, ok := cfg.Groups[gid]
		if ok == false {
			// 这个 GID 本来就不在配置里
			log.Printf("Leave(%v) but not in config", gid)
			return false
		} else {
			// 修改配置以反映 Leave()
			delete(cfg.Groups, gid)
			changed = true
		}
	}
	if changed == false {
		debug.PrintStack()
		log.Fatalf("Leave but no change")
	}
	cfg.Num += 1
	return true
}

// 先加入组，再平衡shard。
func (cfg *ShardConfig) JoinBalance(servers map[tester.Tgid][]string) bool {
	if !cfg.Join(servers) {
		return false
	}
	cfg.Rebalance()
	return true
}

// 先离开组，再平衡shard。
func (cfg *ShardConfig) LeaveBalance(gids []tester.Tgid) bool {
	if !cfg.Leave(gids) {
		return false
	}
	cfg.Rebalance()
	return true
}

// 返回shard所属的group的GID、服务器列表，以及该group是否存在。
func (cfg *ShardConfig) GidServers(sh Tshid) (tester.Tgid, []string, bool) {
	gid := cfg.Shards[sh]
	srvs, ok := cfg.Groups[gid]
	return gid, srvs, ok
}

// 判断一个group是否在配置中。
func (cfg *ShardConfig) IsMember(gid tester.Tgid) bool {
	for _, g := range cfg.Shards {
		if g == gid {
			return true
		}
	}
	return false
}

// 检查配置是否满足条件：
// 配置中的group数量与预期的group数量相同。
// 配置中的group集合与预期的group集合相同。
// 配置中没有未分配的shard。
// shard 的分配大致均衡，即最大 shard 数量不超过最小shard数量加 1。
func (cfg *ShardConfig) CheckConfig(t *testing.T, groups []tester.Tgid) {
	if len(cfg.Groups) != len(groups) {
		fatalf(t, "wanted %v groups, got %v", len(groups), len(cfg.Groups))
	}

	// group 集合是否符合预期？
	for _, g := range groups {
		_, ok := cfg.Groups[g]
		if ok != true {
			fatalf(t, "missing group %v", g)
		}
	}

	// 是否存在未分配的 shard？
	if len(groups) > 0 {
		for s, g := range cfg.Shards {
			_, ok := cfg.Groups[g]
			if ok == false {
				fatalf(t, "shard %v -> invalid group %v", s, g)
			}
		}
	}

	// shard 分配是否大致均衡？
	counts := map[tester.Tgid]int{}
	for _, g := range cfg.Shards {
		counts[g] += 1
	}
	min := 257
	max := 0
	for g, _ := range cfg.Groups {
		if counts[g] > max {
			max = counts[g]
		}
		if counts[g] < min {
			min = counts[g]
		}
	}
	if max > min+1 {
		fatalf(t, "max %v too much larger than min %v", max, min)
	}
}

func fatalf(t *testing.T, format string, args ...any) {
	debug.PrintStack()
	t.Fatalf(format, args...)
}
