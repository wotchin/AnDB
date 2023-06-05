import os

# Constants for buffer management
BUFFER_SIZE = 10


class BufferManager:
    def __init__(self):
        self.buffer = {}
        self.page_directory = {}

    def get_page(self, page_id):
        if page_id in self.buffer:
            page = self.buffer[page_id]
            page.next_free_slot += 1
            return page
        else:
            page = self._read_page_from_disk(page_id)
            if len(self.buffer) >= BUFFER_SIZE:
                victim_page_id = self._select_victim_page()
                self._write_page_to_disk(victim_page_id)
                del self.buffer[victim_page_id]
            self.buffer[page_id] = page
            return page

    def pin_page(self, page_id):
        if page_id in self.buffer:
            self.buffer[page_id].pinned = True

    def unpin_page(self, page_id):
        if page_id in self.buffer:
            self.buffer[page_id].pinned = False

    def flush_buffer(self):
        for page_id in self.buffer:
            self._write_page_to_disk(page_id)
        self.buffer = {}

    def _read_page_from_disk(self, page_id):
        # keynote: another thinking is segment form.
        file_path = f"pages/{page_id}.bin"
        if not os.path.exists(file_path):
            return Page(page_id)
        with open(file_path, "rb") as file:
            data = file.read()
        return Page.from_bytes(data)

    def _write_page_to_disk(self, page_id):
        page = self.buffer[page_id]
        data = page.to_bytes()
        file_path = f"pages/{page_id}.bin"
        with open(file_path, "wb") as file:
            file.write(data)

    def _select_victim_page(self):
        # TODO: Implement the page eviction algorithm (LRU or Clock-Sweep)
        # For now, return a random page as a placeholder
        return next(iter(self.buffer.keys()))

