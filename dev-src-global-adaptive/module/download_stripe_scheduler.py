"""Fair logical-stripe scheduling by Telegram DC and transfer."""

from collections import OrderedDict, deque
from typing import Deque, Dict, Generic, List, Optional, TypeVar

T = TypeVar("T")


class DownloadStripeScheduler(Generic[T]):
    def __init__(self):
        self._items: Dict[int, OrderedDict[str, Deque[T]]] = {}
        self._rotation: Dict[int, Deque[str]] = {}

    def enqueue(self, dc_id: int, transfer_id: str, item: T) -> None:
        owners = self._items.setdefault(dc_id, OrderedDict())
        rotation = self._rotation.setdefault(dc_id, deque())
        if transfer_id not in owners:
            owners[transfer_id] = deque()
            rotation.append(transfer_id)
        owners[transfer_id].append(item)

    def pop_next(self, dc_id: int) -> Optional[T]:
        owners = self._items.get(dc_id)
        rotation = self._rotation.get(dc_id)
        if not owners or not rotation:
            return None
        transfer_id = rotation.popleft()
        queue = owners[transfer_id]
        item = queue.popleft()
        if queue:
            rotation.append(transfer_id)
        else:
            del owners[transfer_id]
        self._prune(dc_id)
        return item

    def cancel(self, item: T) -> bool:
        for dc_id, owners in list(self._items.items()):
            for transfer_id, queue in list(owners.items()):
                try:
                    queue.remove(item)
                except ValueError:
                    continue
                if not queue:
                    self._drop_owner(dc_id, transfer_id)
                self._prune(dc_id)
                return True
        return False

    def remove_transfer(self, dc_id: int, transfer_id: str) -> List[T]:
        owners = self._items.get(dc_id, OrderedDict())
        removed = list(owners.get(transfer_id, ()))
        self._drop_owner(dc_id, transfer_id)
        self._prune(dc_id)
        return removed

    def pending_count(self, dc_id: Optional[int] = None) -> int:
        if dc_id is not None:
            return sum(len(queue) for queue in self._items.get(dc_id, {}).values())
        return sum(
            len(queue)
            for owners in self._items.values()
            for queue in owners.values()
        )

    def active_transfer_count(self, dc_id: int) -> int:
        return len(self._items.get(dc_id, {}))

    def dc_ids(self) -> List[int]:
        return [dc_id for dc_id in self._items if self.pending_count(dc_id)]

    def _drop_owner(self, dc_id: int, transfer_id: str) -> None:
        owners = self._items.get(dc_id)
        rotation = self._rotation.get(dc_id)
        if owners is not None:
            owners.pop(transfer_id, None)
        if rotation is not None:
            self._rotation[dc_id] = deque(
                owner for owner in rotation if owner != transfer_id
            )

    def _prune(self, dc_id: int) -> None:
        if not self._items.get(dc_id):
            self._items.pop(dc_id, None)
            self._rotation.pop(dc_id, None)
