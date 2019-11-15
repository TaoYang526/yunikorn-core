/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduler

import (
    "github.com/cloudera/yunikorn-core/pkg/common"
    "github.com/cloudera/yunikorn-core/pkg/common/resources"
    "github.com/cloudera/yunikorn-core/pkg/log"
    "github.com/cloudera/yunikorn-core/pkg/metrics"
    "github.com/google/btree"
    "go.uber.org/zap"
    "sort"
    "strings"
    "time"
)

// Sort queues, apps, etc.

type SortType int32

const (
    FairSortPolicy        = 0
    FifoSortPolicy        = 1
    MaxAvailableResources = 2
)

func SortQueue(queues []*SchedulingQueue, sortType SortType) {
    if sortType == FairSortPolicy {
        sort.SliceStable(queues, func(i, j int) bool {
            l := queues[i]
            r := queues[j]

            comp := resources.CompUsageRatioSeparately(l.ProposingResource, l.CachedQueueInfo.GuaranteedResource,
                r.ProposingResource, r.CachedQueueInfo.GuaranteedResource)
            return comp < 0
        })
    }
}

func SortApplications(queues []*SchedulingApplication, sortType SortType, globalResource *resources.Resource) {
    if sortType == FairSortPolicy {
        sort.SliceStable(queues, func(i, j int) bool {
            l := queues[i]
            r := queues[j]

            comp := resources.CompUsageRatio(l.MayAllocatedResource, r.MayAllocatedResource, globalResource)
            return comp < 0
        })
    } else if sortType == FifoSortPolicy {
        sort.SliceStable(queues, func(i, j int) bool {
            l := queues[i]
            r := queues[j]
            return l.ApplicationInfo.SubmissionTime < r.ApplicationInfo.SubmissionTime
        })
    }
}

func SortNodes(nodes []*SchedulingNode, sortType SortType) {
    if sortType == MaxAvailableResources {
        sortingStart := time.Now()
        sort.SliceStable(nodes, func(i, j int) bool {
            l := nodes[i]
            r := nodes[j]

            // Sort by available resource, descending order
            return resources.CompUsageShares(l.GetCachedAvailableResource(), r.GetCachedAvailableResource()) > 0
        })
        metrics.GetSchedulerMetrics().ObserveNodeSortingLatency(sortingStart)
    }
}

// Sort nodes here.
func SortAllNodesWithAscendingResource(schedulingNodeList []*SchedulingNode) []*SchedulingNode {

    sortingStart := time.Now()
    sort.SliceStable(schedulingNodeList, func(i, j int) bool {
        l := schedulingNodeList[i]
        r := schedulingNodeList[j]

        // Sort by available resource, ascending order
        return resources.CompUsageShares(r.GetCachedAvailableResource(), l.GetCachedAvailableResource()) > 0
    })

    metrics.GetSchedulerMetrics().ObserveNodeSortingLatency(sortingStart)

    return schedulingNodeList
}

type SchedulingNodeSnapshot struct {
    schedulingNode          *SchedulingNode
    cachedAvailableResource *resources.Resource
}

func (m *SchedulingNodeSnapshot) Less(than btree.Item) bool {
    availSharesComp := resources.CompUsageShares(m.cachedAvailableResource, than.(*SchedulingNodeSnapshot).cachedAvailableResource)
    if availSharesComp != 0 {
        return availSharesComp < 0
    }
    return strings.Compare(m.schedulingNode.NodeId, than.(*SchedulingNodeSnapshot).schedulingNode.NodeId) > 0
}

type NodeSorter struct {
    nodeTree *btree.BTree
    nodes    map[string]*SchedulingNodeSnapshot
}

func NewNodeSorter(schedulingNodes []*SchedulingNode) *NodeSorter {
    nodeSorter := &NodeSorter{
        nodeTree: btree.New(32),
        nodes: make(map[string]*SchedulingNodeSnapshot),
    }
    for _, schedulingNode := range schedulingNodes {
        nodeSorter.AddSchedulingNode(schedulingNode)
    }
    return nodeSorter
}

func (m *NodeSorter) AddSchedulingNode(schedulingNode *SchedulingNode) {
    nodeSnapshot := &SchedulingNodeSnapshot{
        schedulingNode:          schedulingNode,
        cachedAvailableResource: schedulingNode.cachedAvailableResource.Clone(),
    }
    m.nodeTree.ReplaceOrInsert(nodeSnapshot)
    m.nodes[schedulingNode.NodeId] = nodeSnapshot
}

func (m *NodeSorter) ResortNode(nodeId string) bool {
    if nodeSnapshot, ok := m.nodes[nodeId]; ok {
        deletedItem := m.nodeTree.Delete(nodeSnapshot)
        if deletedItem == nil {
            log.Logger().Warn("Can't find node snapshot in node sorter",
                zap.String("nodeId", nodeId))
        } else {
            schedulingNode := deletedItem.(*SchedulingNodeSnapshot).schedulingNode
            m.AddSchedulingNode(schedulingNode)
            return true
        }
    } else {
        log.Logger().Warn("Can't find node snapshot in node sorter",
            zap.String("nodeId", nodeId))
    }
    return false
}

func (m *NodeSorter) GetSortedSchedulingNodes(configuredPolicy common.SortingPolicy) []*SchedulingNode {
    var sortedSchedulingNodes []*SchedulingNode
    switch configuredPolicy {
    case common.BinPackingPolicy:
        m.nodeTree.Ascend(func(i btree.Item) bool {
            sortedSchedulingNodes = append(sortedSchedulingNodes, i.(*SchedulingNodeSnapshot).schedulingNode)
            return true
        })
    case common.FairnessPolicy:
        m.nodeTree.Descend(func(i btree.Item) bool {
            sortedSchedulingNodes = append(sortedSchedulingNodes, i.(*SchedulingNodeSnapshot).schedulingNode)
            return true
        })
    }
    return sortedSchedulingNodes
}