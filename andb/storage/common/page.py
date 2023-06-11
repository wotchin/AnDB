from functools import lru_cache

from andb.common.cstructure import CStructure
from andb.common.cstructure import Integer8Field
from andb.common.cstructure import Integer4Field

from andb.constants.values import PAGE_SIZE
from andb.common.utils import is_array_like


class PageHeader(CStructure):
    lsn: int = Integer8Field()
    checksum: int = Integer4Field()
    flags: int = Integer4Field()
    reserved: int = Integer4Field()
    lower: int = Integer4Field()
    upper: int = Integer4Field()


class ItemIdFlags:
    UNUSED = 0
    NORMAL = 1
    REDIRECT = 2
    DEAD = 3


MASKER_15BITS = 0b111111111111111
MASKER_2BITS = 0b11


class ItemIdData:
    # This is a structure of echo element in the slot array,
    # which structure is the following:

    # | <---------------------- int 4 bytes -------------------------> |
    # +----------------------------------------------------------------+
    # |  offset <15bits>       | flag <2bits> |  offset <15bits>       |
    # +----------------------------------------------------------------+
    BYTES = 4
    TOTAL_BITS = BYTES * 8  # sizeof(int)
    OFFSET_BITS = 15
    FLAGS_BITS = 2
    LENGTH_BITS = 15

    OFFSET_SHIFT = FLAGS_BITS + LENGTH_BITS
    FLAGS_SHIFT = LENGTH_BITS
    LENGTH_SHIFT = 0

    def __init__(self, offset=0, flags=0, length=0):
        # offset of one tuple
        self.offset = offset
        # a flag of ItemIdFlags
        self.flag = flags
        # the length/size of a tuple
        self.length = length

    def to_uint4(self):
        return ((self.offset << self.OFFSET_SHIFT) |
                (self.flag << self.FLAGS_SHIFT) |
                (self.length << self.LENGTH_SHIFT))

    def set_uint4(self, value):
        self.offset = (value & (MASKER_15BITS << self.OFFSET_SHIFT)) >> self.OFFSET_SHIFT
        self.flag = (value & (MASKER_2BITS << self.FLAGS_SHIFT)) >> self.FLAGS_SHIFT
        self.length = (value & (MASKER_15BITS << self.LENGTH_SHIFT)) >> self.LENGTH_SHIFT

    def __eq__(self, other):
        if not isinstance(other, ItemIdData):
            return False
        return self.to_uint4() == other.to_uint4()

    def __hash__(self):
        return self.to_uint4()


INVALID_ITEM_ID = -1
INVALID_BYTES = bytes()


@lru_cache(maxsize=8)
def _get_item_ids_parser(length):
    class ItemIdsTemp(CStructure):
        item_ids = Integer4Field(unsigned=True, num=length)

    obj = ItemIdsTemp()

    class Inner:
        @staticmethod
        def pack(uint4_list):
            # CString doesn't receive an array contains one element, so we should
            # covert `uint4_list` to adapt this scenario.
            if length == 1 and is_array_like(uint4_list):
                obj.item_ids = uint4_list[0]
            else:
                obj.item_ids = uint4_list
            return obj.pack()

        @staticmethod
        def unpack(data):
            obj.unpack(data)
            # CString doesn't receive an array contains one element, so we should
            # wrap `item_ids` using a list.
            if length == 1:
                return [obj.item_ids]
            return obj.item_ids

    return Inner()


class Page:
    def __init__(self):
        """The page structure likes below:

        -----------------------------------------------------------
        lsn | checksum | flags | lower | upper | ... item_ids ... |
        ... free space ... | itemN |     .........      | item0  |
        ---------------------------------------------------------
        """
        self.header = PageHeader()
        self.item_ids = []  # array of ItemIdData
        # Here, we use bytearray instead of bytes since bytearray is mutable but
        # bytes is immutable. That means, if we use bytes, we have to copy-on-write.
        # This copy-on-write operation will introduce lots of overhead.
        self.items = bytearray()

    def item_data_size(self):
        return len(self.items)

    def item_ids_size(self):
        # naturally byte-aligned
        return len(self.item_ids) * ItemIdData.BYTES

    def free_space_size(self):
        return (PAGE_SIZE - self.header.size() -
                self.item_ids_size() - self.item_data_size())

    def can_put_item(self, data: bytes):
        """Validate current free space can accommodate an item data
        and its ID tag."""
        return len(data) <= (self.free_space_size() - ItemIdData.BYTES)

    def insert(self, lsn: int, data: bytes) -> int:
        """Put into item data and return its item pointer.
        :param lsn: LSN for this insertion
        :param data: item data, bytes
        :return: item pointer, int. If it failed, return INVALID_ITEM_ID.
        """
        if len(data) == 0 or not self.can_put_item(data):
            return INVALID_ITEM_ID

        length = len(data)
        offset = self.header.upper - length
        flag = ItemIdFlags.NORMAL

        # allocate new ItemId
        self.item_ids.append(ItemIdData(offset=offset, flags=flag, length=length))
        # then, put the data into the items array
        self.items = bytearray(data) + self.items  # put into the first place
        # finally, update some fields
        self.header.lsn = lsn
        self.header.checksum = 0  # todo: unset
        self.header.lower = self.header.size() + self.item_ids_size()
        self.header.upper = PAGE_SIZE - self.item_data_size()
        # return just put index of the whole item_ids list
        return len(self.item_ids) - 1

    def delete(self, lsn: int, item_idx: int) -> bool:
        """We employ mark-and-sweep method to delete an item.
        :param lsn: LSN for this delete
        :param item_idx: the index of `self.item_ids` list
        :return: if it is successful, returns true, otherwise, return false.
        """
        if not (0 <= item_idx < len(self.item_ids)):
            return False
        item_id: ItemIdData = self.item_ids[item_idx]
        # we only can delete valid (normal) item
        if item_id.flag != ItemIdFlags.NORMAL:
            return False

        self.header.lsn = lsn
        item_id.flag = ItemIdFlags.DEAD
        self.header.checksum = 0  # todo: unset
        return True

    def rollback_delete(self, old_lsn: int, item_idx: int) -> bool:
        if not (0 <= item_idx < len(self.item_ids)):
            return False
        item_id: ItemIdData = self.item_ids[item_idx]
        # we only can delete valid (normal) item
        if item_id.flag != ItemIdFlags.DEAD:
            return False

        self.header.lsn = old_lsn
        item_id.flag = ItemIdFlags.NORMAL
        self.header.checksum = 0  # todo: unset
        return True

    def select(self, item_idx: int) -> bytes:
        """Return bytes according to parameter item_idx.
        :param item_idx: the index of `self.item_ids` list
        :return: if it is successful, returns item bytes, otherwise,
          returns INVALID_BYTES.
        """
        if not (0 <= item_idx < len(self.item_ids)):
            return INVALID_BYTES
        item_id: ItemIdData = self.item_ids[item_idx]
        if item_id.flag != ItemIdFlags.NORMAL:
            return INVALID_BYTES
        offset = item_id.offset
        length = item_id.length

        # the offset we record is relative to the entire page,
        # but we need to convert the offset in the filed `self.items`.
        # this is a small formula to get data bytes
        offset_of_items = self.item_data_size() - (PAGE_SIZE - offset)
        item_data = bytes(self.items[offset_of_items: (offset_of_items + length)])
        return item_data

    def update(self, lsn: int, item_idx: int, data: bytes) -> int:
        """We employ both inplace-update and append-only. If the data size is the same,
        we use inplace-update method directly. Otherwise, we use append-only method.
        :param lsn: LSN for updating
        :param item_idx: the index of `self.item_ids` list
        :param data: new item data
        """
        if not (0 <= item_idx < len(self.item_ids)):
            return INVALID_ITEM_ID
        item_id: ItemIdData = self.item_ids[item_idx]
        # we only can update valid (normal) item
        if item_id.flag != ItemIdFlags.NORMAL:
            return INVALID_ITEM_ID

        length = item_id.length
        if len(data) == length:
            # use inplace-update
            offset = item_id.offset
            offset_of_items = self.item_data_size() - (PAGE_SIZE - offset)
            # The following is bytearray's benefit.
            self.items[offset_of_items: (offset_of_items + length)] = data
            self.header.lsn = lsn
            self.header.checksum = 0  # todo: unset
            return item_idx
        else:
            # use append-only
            # we should keep atomic in this method
            old_lsn = self.header.lsn
            delete_ok = self.delete(lsn, item_idx)
            if delete_ok:
                new_item_idx = self.insert(lsn, data)
                if new_item_idx == INVALID_ITEM_ID:
                    rollback_delete_ok = self.rollback_delete(old_lsn, item_idx)
                    if not rollback_delete_ok:
                        assert False, "BUG!!!"
                    return INVALID_ITEM_ID
                return new_item_idx
        return INVALID_ITEM_ID

    def vacuum(self, lsn: int):
        """Clean up dead items, aka, reorganize"""
        new_item_ids = list()
        # first, pick up
        for item_id in self.item_ids:
            if item_id.flag != ItemIdFlags.DEAD:
                new_item_ids.append(item_id)
        # then, move normal items together and replace old one
        # although, the following implementation is not inplace modification for
        # the item data list. It is easy to implement and friendly to concurrency and
        # performance may be higher due to underlying implementation?
        new_data = bytes()
        new_upper = PAGE_SIZE
        for item_id in new_item_ids:
            offset = item_id.offset
            length = item_id.length
            offset_of_items = self.item_data_size() - (PAGE_SIZE - offset)
            item_data = self.items[offset_of_items: (offset_of_items + length)]
            new_data = item_data + new_data
            # Okay, data have been recorded. Later, we should to update the ID information.
            item_id.offset = new_upper - length
            new_upper = item_id.offset
        # finally, we should switch old `self.items` to new one.
        # === the following will be a critical section if it is in concurrency scenario ===
        # todo: spin lock
        self.item_ids = new_item_ids
        self.items = new_data
        self.header.lsn = lsn
        self.header.checksum = 0  # todo: unset
        self.header.upper = new_upper
        self.header.lower = self.header.size() + self.item_ids_size()
        # === end critical section ===

    def reset(self, lsn: int):
        """Reset the page likes delete all items."""
        self.item_ids.clear()
        self.items = bytes()

        self.header.lsn = lsn
        self.header.lower = self.header.size() + self.item_ids_size()
        self.header.upper = PAGE_SIZE
        self.header.checksum = 0  # todo: unset

    def pack(self) -> bytes:
        """Serialize the class to bytes."""
        serialized_data = self.header.pack()
        serialized_data += _get_item_ids_parser(len(self.item_ids)).pack(
            [item_id.to_uint4() for item_id in self.item_ids]
        )
        serialized_data += bytes(self.free_space_size())
        serialized_data += self.items

        assert len(serialized_data) == PAGE_SIZE
        return serialized_data

    @staticmethod
    def unpack(data: bytes):
        """Deserialize bytes to a class."""
        page = Page()
        deserialized_position = 0
        # parse header
        page_header_size = page.header.size()
        chunk = data[deserialized_position: (deserialized_position + page_header_size)]
        page.header.unpack(chunk)
        deserialized_position += len(chunk)
        # parse item ids
        item_ids_size = page.header.lower - page_header_size
        chunk = data[deserialized_position: (deserialized_position + item_ids_size)]
        for id_uint4 in _get_item_ids_parser(len(chunk) // ItemIdData.BYTES).unpack(chunk):
            item_id = ItemIdData()
            item_id.set_uint4(id_uint4)
            page.item_ids.append(item_id)
        # parse item data
        page.items = data[page.header.upper:]
        return page

    @staticmethod
    def allocate(lsn=0):
        page = Page()
        page.header.lsn = lsn
        page.header.flags = 0
        page.header.lower = page.header.size() + page.item_ids_size()
        page.header.upper = PAGE_SIZE
        page.header.checksum = 0  # todo: unset
        return page

    def __eq__(self, other):
        if not isinstance(other, Page):
            return False
        return (other.item_ids == self.item_ids) and (
            other.header == self.header
        ) and (other.items == self.items)

    def __hash__(self):
        return hash((tuple(self.item_ids), self.header.pack(), self.items))

