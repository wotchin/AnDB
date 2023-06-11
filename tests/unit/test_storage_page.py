from andb.storage.common.page import (
    ItemIdFlags,
    ItemIdData,
    MASKER_15BITS,
    INVALID_BYTES,
    INVALID_ITEM_ID,
    Page
)
from andb.constants.values import PAGE_SIZE


def test_item_id_data():
    offset = 100
    flag = ItemIdFlags.UNUSED
    length = 1222

    a = ItemIdData(offset, flag, length)
    a_uint4 = a.to_uint4()
    assert a_uint4 <= 0xffffffff
    b = ItemIdData(0, 0, 0)
    assert b.to_uint4() == 0
    b.set_uint4(a_uint4)
    assert b.offset == offset
    assert b.flag == flag
    assert b.length == length

    assert PAGE_SIZE <= MASKER_15BITS


def fixture_page_insert(page, times=4):
    # test page insert
    lsn = page.header.lsn
    start_lsn = lsn
    empty_free_space_size = page.free_space_size()
    insertion_item_size = 0
    for _ in range(times):
        lsn += 1
        insert_data = ('0x%02x' % lsn).encode()
        insert_data_len = len(insert_data)
        page.insert(lsn, insert_data)
        insertion_item_size += insert_data_len

    after_insertion_free_space_size = page.free_space_size()
    # after_insertion_free_space_size = page.free_space_size()
    # test free space size
    assert empty_free_space_size == (
            after_insertion_free_space_size + insertion_item_size + times * ItemIdData.BYTES
    )
    item_data_size = 0
    for item_id in page.item_ids:
        item_data_size += item_id.length
    assert page.item_data_size() == item_data_size
    assert page.header.lsn == start_lsn + times


def fixture_page_delete(page):
    # test basic delete
    # first of all, insert 100 items
    selected_values = {}
    for idx in range(len(page.item_ids)):
        selected_values[idx] = page.select(idx)

    fixture_page_insert(page, 100)

    for idx in range(len(page.item_ids)):
        if idx in selected_values:
            assert selected_values[idx] == page.select(idx)
        selected_values[idx] = page.select(idx)

    item_ids_len = len(page.item_ids)
    # then, delete some items
    delete_idx = (0, 30, 40, 41, 90, 95, 96, item_ids_len - 1)

    lsn = page.header.lsn
    deleted_size = 0
    for idx in delete_idx:
        lsn += 1
        deleted_size += (page.item_ids[idx].length + ItemIdData.BYTES)
        assert page.delete(lsn, idx)
        assert page.item_ids[idx].flag == ItemIdFlags.DEAD
        assert not page.delete(lsn, idx)

    # test rollback_delete
    assert page.delete(lsn + 1, 1)
    assert page.select(1) == INVALID_BYTES
    assert page.rollback_delete(lsn, 1)
    assert page.header.lsn == lsn
    assert page.select(1) != INVALID_BYTES

    for idx in range(len(page.item_ids)):
        if idx in delete_idx:
            assert page.select(idx) == INVALID_BYTES
        else:
            assert page.select(idx) == selected_values[idx]

    lsn += 1
    size_with_dead_item = page.free_space_size()
    nb_idx_with_dead_item = len(page.item_ids)
    assert (page.header.size() + page.free_space_size() + page.item_data_size() + page.item_ids_size()) == PAGE_SIZE
    page.vacuum(lsn)
    assert (page.header.size() + page.free_space_size() + page.item_data_size() + page.item_ids_size()) == PAGE_SIZE
    assert len(page.item_ids) + len(delete_idx) == nb_idx_with_dead_item
    assert page.free_space_size() - size_with_dead_item == deleted_size

    # because we delete the first element, all of data should have changed.
    for idx in range(len(page.item_ids)):
        assert page.select(idx) != selected_values[idx]


def fixture_page_update(page):
    lsn = page.header.lsn
    idx0 = 0
    item0 = page.select(idx0)
    lsn += 1
    # set the same length empty bytes
    assert page.update(lsn, idx0, bytes(len(item0))) == idx0
    assert page.select(idx0) == bytes(len(item0))
    # because it used inplace-update method, we cannot rollback the delete operation
    assert not page.rollback_delete(lsn - 1, idx0)
    assert page.header.lsn == lsn
    # test append only
    before_updating_size = page.free_space_size()
    new_idx = page.update(lsn, idx0, bytes(len(item0) * 2))
    assert new_idx not in (INVALID_ITEM_ID, idx0)
    assert page.free_space_size() < before_updating_size
    assert page.select(new_idx) == bytes(len(item0) * 2)

    idx0 = new_idx
    # test update huge page
    new_idx = page.update(lsn + 1, idx0, bytes(page.free_space_size()))
    assert new_idx == INVALID_ITEM_ID
    assert page.header.lsn == lsn
    huge_bytes = bytes(page.free_space_size() - ItemIdData.BYTES)
    new_idx = page.update(lsn + 1, idx0, huge_bytes)
    assert new_idx != INVALID_ITEM_ID
    assert page.select(new_idx) == huge_bytes


def fixture_pack_and_unpack(page):
    page0 = page
    page1 = page.unpack(page.pack())
    assert page0 == page1


def test_page():
    start_lsn = 0
    page0 = Page.allocate(start_lsn)
    fixture_page_insert(page0, times=10)
    fixture_page_delete(page0)
    fixture_page_update(page0)
    fixture_pack_and_unpack(page0)
