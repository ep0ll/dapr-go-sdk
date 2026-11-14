/*
Copyright 2026 The Dapr Authors
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

package client

// ActorStateETagMismatchError is returned when an actor state transaction
// uses an ETag that no longer matches the stored value.
type ActorStateETagMismatchError struct {
	ActorType string
	ActorID   string
	Err       error
}

func (e *ActorStateETagMismatchError) Error() string {
	if e.Err != nil {
		return "actor state etag mismatch: " + e.Err.Error()
	}
	return "actor state etag mismatch"
}

func (e *ActorStateETagMismatchError) Unwrap() error {
	return e.Err
}
