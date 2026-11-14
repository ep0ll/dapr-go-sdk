/*
Copyright 2021 The Dapr Authors
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

package state

import (
	"context"
	"fmt"
	"sync"

	"github.com/dapr/go-sdk/actor/codec"
	"github.com/dapr/go-sdk/actor/codec/constant"
	client "github.com/dapr/go-sdk/client"
)

type DaprStateAsyncProvider struct {
	daprClient      client.Client
	stateSerializer codec.Codec

	etagMu sync.RWMutex
	etags  map[string]string
}

// Deprecated: use ContainsContext instead.
func (d *DaprStateAsyncProvider) Contains(actorType string, actorID string, stateName string) (bool, error) {
	return d.ContainsContext(context.Background(), actorType, actorID, stateName)
}

func (d *DaprStateAsyncProvider) ContainsContext(ctx context.Context, actorType string, actorID string, stateName string) (bool, error) {
	result, err := d.daprClient.GetActorState(ctx, &client.GetActorStateRequest{
		ActorType: actorType,
		ActorID:   actorID,
		KeyName:   stateName,
	})
	if err != nil {
		return false, err
	}
	if result == nil {
		d.rememberETag(actorType, actorID, stateName, "")
		return false, nil
	}
	d.rememberETag(actorType, actorID, stateName, result.ETag)
	return len(result.Data) > 0, nil
}

// Deprecated: use LoadContext instead.
func (d *DaprStateAsyncProvider) Load(actorType, actorID, stateName string, reply interface{}) error {
	return d.LoadContext(context.Background(), actorType, actorID, stateName, reply)
}

func (d *DaprStateAsyncProvider) LoadContext(ctx context.Context, actorType, actorID, stateName string, reply interface{}) error {
	result, err := d.daprClient.GetActorState(ctx, &client.GetActorStateRequest{
		ActorType: actorType,
		ActorID:   actorID,
		KeyName:   stateName,
	})
	if err != nil {
		return fmt.Errorf("get actor state error = %w", err)
	}
	if result == nil {
		d.rememberETag(actorType, actorID, stateName, "")
		return fmt.Errorf("get actor state result is nil, with actorType: %s, actorID: %s, stateName %s", actorType, actorID, stateName)
	}
	d.rememberETag(actorType, actorID, stateName, result.ETag)
	if len(result.Data) == 0 {
		return fmt.Errorf("get actor state result empty, with actorType: %s, actorID: %s, stateName %s", actorType, actorID, stateName)
	}
	if err := d.stateSerializer.Unmarshal(result.Data, reply); err != nil {
		return fmt.Errorf("unmarshal state data error = %w", err)
	}
	return nil
}

// Deprecated: use ApplyContext instead.
func (d *DaprStateAsyncProvider) Apply(actorType, actorID string, changes []*ActorStateChange) error {
	return d.ApplyContext(context.Background(), actorType, actorID, changes)
}

func (d *DaprStateAsyncProvider) ApplyContext(ctx context.Context, actorType, actorID string, changes []*ActorStateChange) error {
	if len(changes) == 0 {
		return nil
	}

	operations := make([]*client.ActorStateOperation, 0)
	for _, stateChange := range changes {
		if stateChange == nil {
			continue
		}

		daprOperationName := string(stateChange.changeKind)
		if len(daprOperationName) == 0 {
			continue
		}

		var operationValue []byte
		if stateChange.changeKind == Add || stateChange.changeKind == Update {
			data, err := d.stateSerializer.Marshal(stateChange.value)
			if err != nil {
				return err
			}
			operationValue = data
		}
		operations = append(operations, &client.ActorStateOperation{
			OperationType: daprOperationName,
			Key:           stateChange.stateName,
			Value:         operationValue,
			TTLInSeconds:  stateChange.ttlInSeconds,
			ETag:          d.getETag(actorType, actorID, stateChange.stateName),
		})
	}

	if len(operations) == 0 {
		return nil
	}

	if err := d.daprClient.SaveStateTransactionally(ctx, actorType, actorID, operations); err != nil {
		return err
	}
	for _, change := range changes {
		if change != nil {
			d.deleteETag(actorType, actorID, change.stateName)
		}
	}
	return nil
}

func (d *DaprStateAsyncProvider) etagKey(actorType, actorID, stateName string) string {
	return fmt.Sprintf("%d:%s%d:%s%s", len(actorType), actorType, len(actorID), actorID, stateName)
}

func (d *DaprStateAsyncProvider) rememberETag(actorType, actorID, stateName, etag string) {
	key := d.etagKey(actorType, actorID, stateName)
	d.etagMu.Lock()
	defer d.etagMu.Unlock()
	if etag == "" {
		delete(d.etags, key)
		return
	}
	if d.etags == nil {
		d.etags = make(map[string]string)
	}
	d.etags[key] = etag
}

func (d *DaprStateAsyncProvider) getETag(actorType, actorID, stateName string) string {
	d.etagMu.RLock()
	defer d.etagMu.RUnlock()
	return d.etags[d.etagKey(actorType, actorID, stateName)]
}

func (d *DaprStateAsyncProvider) deleteETag(actorType, actorID, stateName string) {
	d.etagMu.Lock()
	delete(d.etags, d.etagKey(actorType, actorID, stateName))
	d.etagMu.Unlock()
}

// TODO(@laurence) the daprClient may be nil.
func NewDaprStateAsyncProvider(daprClient client.Client) *DaprStateAsyncProvider {
	stateSerializer, _ := codec.GetActorCodec(constant.DefaultSerializerType)
	return &DaprStateAsyncProvider{
		stateSerializer: stateSerializer,
		daprClient:      daprClient,
	}
}
