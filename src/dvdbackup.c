/*
 * dvdbackup - tool to rip DVDs from the command line
 *
 * Copyright (C) 2002  Olaf Beck <olaf_sc@yahoo.com>
 * Copyright (C) 2008-2013  Benjamin Drung <benjamin.drung@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <config.h>
#include "dvdbackup.h"

/* internationalisation */
#include "gettext.h"
#define _(String) gettext(String)

/* C standard libraries */
#include <ctype.h>
#include <limits.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* C POSIX library */
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

/* libdvdread */
#include <dvdread/dvd_reader.h>
#include <dvdread/ifo_read.h>


#define MAXNAME 256

/**
 * Buffer size in DVD logical blocks (2 KiB).
 * Currently set to 1 MiB.
 */
#define BUFFER_SIZE 512

/**
 * The maximum size of a VOB file is 1 GiB or 524288 in Video DVD logical block
 * respectively.
 */
#define MAX_VOB_SIZE 524288

/* Number of verification samples to collect when refreshing with --gaps. */
#define GAP_SAMPLE_TARGET 32

#define DVD_SEC_SIZ 2048

/* Flag for verbose mode */
int verbose = 0;
int aspect;
int progress = 0;
char progressText[MAXNAME] = "n/a";
int fill_gaps = 0;
int no_overwrite = 0;
gap_strategy_t gap_strategy = GAP_STRATEGY_FORWARD;
unsigned int gap_random_seed = 0;
int gap_random_seed_set = 0;
int compare_only = 0;
int gap_map = 0;

/* Structs to keep title set information in */

typedef struct {
	off_t size_ifo;
	off_t size_menu;
	int number_of_vob_files;
	off_t size_vob[10];
} title_set_t;

typedef struct {
	int number_of_title_sets;
	title_set_t* title_set;
} title_set_info_t;


typedef struct {
	int title;
	int title_set;
	int vts_title;
	int chapters;
	int aspect_ratio;
	int angles;
	int audio_tracks;
	int audio_channels;
	int sub_pictures;
} titles_t;

typedef struct {
	int main_title_set;
	int number_of_titles;
	titles_t* titles;
} titles_info_t;


static void bsort_max_to_min(int sector[], int title[], int size);

typedef struct {
	size_t start_block;
	size_t block_count;
} gap_fill_segment_t;

static int gap_process_segment(int fd, dvd_file_t* dvd_file, int dvd_offset,
		size_t segment_start, size_t block_count, const char* filename,
		read_error_strategy_t errorstrat, unsigned char* buffer,
		size_t* filled_blocks_out);


static int buffer_is_blank(const unsigned char* buffer, size_t length) {
	size_t i;

	for (i = 0; i < length; ++i) {
		if (buffer[i] != 0x00) {
			return 0;
		}
	}

	return 1;
}


static void report_gap_stats(const char* path, size_t total_blocks, size_t blank_before, size_t blank_after) {
	if (!fill_gaps) {
		return;
	}

	if (total_blocks == 0) {
		printf(_("Gaps stats for %s: no sectors examined\n"), path);
		return;
	}

	double before_pct = ((double)blank_before * 100.0) / (double)total_blocks;
	double after_pct = ((double)blank_after * 100.0) / (double)total_blocks;

	printf(_("Gaps stats for %s: blank before %.2f%%, after %.2f%%\n"), path, before_pct, after_pct);
}


static int finalize_vob_file(int streamout, const char* path, size_t size_blocks,
		size_t total_blocks, size_t blank_before, size_t blank_after) {
	off_t target_size;

	if (streamout == -1) {
		return 0;
	}

	if (fill_gaps) {
		return 0;
	}

	target_size = (off_t)size_blocks * DVD_VIDEO_LB_LEN;
	if (ftruncate(streamout, target_size) != 0) {
		fprintf(stderr, _("Failed to truncate %s\n"), path);
		perror(PACKAGE);
		return -1;
	}

	report_gap_stats(path, total_blocks, blank_before, blank_after);

	return 0;
}


static ssize_t read_existing_range(int fd, off_t offset, unsigned char* buffer, size_t length) {
	size_t total = 0;

	if (length == 0) {
		return 0;
	}

	if (lseek(fd, offset, SEEK_SET) == (off_t)-1) {
		return -1;
	}

	while (total < length) {
		ssize_t read_now = read(fd, buffer + total, length - total);
		if (read_now < 0) {
			if (errno == EINTR) {
				continue;
			}
			return -1;
		}
		if (read_now == 0) {
			break;
		}
		total += (size_t)read_now;
	}

	if (total < length) {
		memset(buffer + total, 0x00, length - total);
	}

	return (ssize_t)total;
}


static ssize_t read_fully(int fd, unsigned char* buffer, size_t length) {
	size_t total = 0;

	while (total < length) {
		ssize_t got = read(fd, buffer + total, length - total);
		if (got < 0) {
			if (errno == EINTR) {
				continue;
			}
			return -1;
		}
		if (got == 0) {
			return (ssize_t)total;
		}
		total += (size_t)got;
	}

	return (ssize_t)total;
}


static int write_range(int fd, off_t offset, const unsigned char* data, size_t length) {
	size_t total = 0;

	if (length == 0) {
		return 0;
	}

	if (lseek(fd, offset, SEEK_SET) == (off_t)-1) {
		return -1;
	}

	while (total < length) {
		ssize_t written = write(fd, data + total, length - total);
		if (written < 0) {
			if (errno == EINTR) {
				continue;
			}
			return -1;
		}
		total += (size_t)written;
	}

	return 0;
}

typedef struct {
	size_t start_block;
	size_t block_count;
} gap_range_t;

typedef struct {
	gap_range_t* ranges;
	size_t count;
	size_t capacity;
} gap_plan_t;

typedef struct {
	size_t start_block;
	size_t block_count;
} gap_map_entry_t;

typedef struct {
	gap_map_entry_t* entries;
	size_t count;
	size_t capacity;
} gap_map_info_t;

static gap_map_info_t gap_map_info = {0};
static size_t gap_map_total_blocks = 0;


static void gap_plan_free(gap_plan_t* plan) {
	free(plan->ranges);
	plan->ranges = NULL;
	plan->count = 0;
	plan->capacity = 0;
}


static int gap_plan_add(gap_plan_t* plan, size_t start, size_t count) {
	gap_range_t* last;
	size_t last_end;
	gap_range_t* new_ranges;
	size_t new_capacity;

	if (count == 0) {
		return 0;
	}

	if (plan->count > 0) {
		last = &plan->ranges[plan->count - 1];
		last_end = last->start_block + last->block_count;
		if (start <= last_end) {
			size_t new_end = start + count;
			if (new_end > last_end) {
				last->block_count = new_end - last->start_block;
			}
			return 0;
		}
	}

	if (plan->count == plan->capacity) {
		new_capacity = plan->capacity == 0 ? 8 : plan->capacity * 2;
		if (new_capacity < plan->count + 1) {
			new_capacity = plan->count + 1;
		}
		new_ranges = realloc(plan->ranges, new_capacity * sizeof(*new_ranges));
		if (new_ranges == NULL) {
			return -1;
		}
		plan->ranges = new_ranges;
		plan->capacity = new_capacity;
	}

	plan->ranges[plan->count].start_block = start;
	plan->ranges[plan->count].block_count = count;
	plan->count++;

	return 0;
}


static int gap_plan_contains(const gap_plan_t* plan, size_t block) {
	size_t i;

	for (i = 0; i < plan->count; ++i) {
		const gap_range_t* range = &plan->ranges[i];
		if (block < range->start_block) {
			return 0;
		}
		if (block < range->start_block + range->block_count) {
			return 1;
		}
	}

	return 0;
}


void gap_map_reset(void) {
	free(gap_map_info.entries);
	gap_map_info.entries = NULL;
	gap_map_info.count = 0;
	gap_map_info.capacity = 0;
	gap_map_total_blocks = 0;
}


static int gap_map_add_entry(size_t start_block, size_t block_count) {
	gap_map_entry_t* entry;

	if (block_count == 0) {
		return 0;
	}

	if (gap_map_info.count == gap_map_info.capacity) {
		size_t new_capacity = gap_map_info.capacity == 0 ? 32 : gap_map_info.capacity * 2;
		gap_map_entry_t* new_entries = realloc(gap_map_info.entries, new_capacity * sizeof(*new_entries));
		if (new_entries == NULL) {
			return -1;
		}
		gap_map_info.entries = new_entries;
		gap_map_info.capacity = new_capacity;
	}

	entry = &gap_map_info.entries[gap_map_info.count++];
	entry->start_block = start_block;
	entry->block_count = block_count;
	return 0;
}


static int gap_map_collect_from_plan(size_t base_block, size_t expected_blocks,
		const gap_plan_t* plan, size_t existing_blocks) {
	size_t i;

	for (i = 0; i < plan->count; ++i) {
		if (gap_map_add_entry(base_block + plan->ranges[i].start_block,
				plan->ranges[i].block_count) != 0) {
			return -1;
		}
	}

	if (existing_blocks < expected_blocks) {
		size_t missing = expected_blocks - existing_blocks;
		if (gap_map_add_entry(base_block + existing_blocks, missing) != 0) {
			return -1;
		}
	}

	return 0;
}


static int gap_map_collect_missing(size_t base_block, size_t expected_blocks) {
	return gap_map_add_entry(base_block, expected_blocks);
}


void gap_map_render(void) {
	const int rows = 20;
	const int cols = 60;
	char map[20][60];
	size_t i;
	const size_t inner_turn = 192;
	const size_t outer_turn = 432;

	if (gap_map_total_blocks == 0) {
		printf(_("Gap map: no sectors examined.\n"));
		return;
	}

	for (int r = 0; r < rows; ++r) {
		for (int c = 0; c < cols; ++c) {
			map[r][c] = '.';
		}
	}

	for (i = 0; i < gap_map_info.count; ++i) {
		size_t start = gap_map_info.entries[i].start_block;
		size_t end = start + gap_map_info.entries[i].block_count;
		size_t span = gap_map_info.entries[i].block_count;
		size_t step = span / ((size_t)cols / 2 + 1);
		if (step == 0) {
			step = 1;
		}
		for (size_t block = start; block < end; block += step) {
			size_t relative = block;
			if (relative >= gap_map_total_blocks) {
				relative = gap_map_total_blocks - 1;
			}
			size_t row_index = (relative * (size_t)rows) / gap_map_total_blocks;
			if (row_index >= (size_t)rows) {
				row_index = (size_t)rows - 1;
			}
			size_t turn_range;
			if (rows > 1) {
				size_t numerator = (outer_turn - inner_turn) * row_index;
				size_t denom = (size_t)rows - 1;
				size_t delta = denom ? numerator / denom : 0;
				turn_range = inner_turn + delta;
			} else {
				turn_range = inner_turn;
			}
			if (turn_range == 0) {
				turn_range = 1;
			}
			size_t pos_in_turn = relative % turn_range;
			int col = (int)((pos_in_turn * (size_t)cols) / turn_range);
			if (col >= cols) {
				col = cols - 1;
			}
			int row = (int)row_index;
			map[row][col] = '#';
		}
	}

	printf(_("Gap map (rows = inner to outer radius, columns = approximate angle):\n"));
	for (int r = 0; r < rows; ++r) {
		printf("|");
		for (int c = 0; c < cols; ++c) {
			putchar(map[r][c]);
		}
		printf("|\n");
	}
	printf(_("# marks sectors that appear blank or missing. Angle is estimated using an average turn length.\n"));
}


void gap_map_free(void) {
	free(gap_map_info.entries);
	gap_map_info.entries = NULL;
	gap_map_info.count = 0;
	gap_map_info.capacity = 0;
	gap_map_total_blocks = 0;
}


static int gap_process_segment(int fd, dvd_file_t* dvd_file, int dvd_offset,
		size_t segment_start, size_t block_count, const char* filename,
		read_error_strategy_t errorstrat, unsigned char* buffer,
		size_t* filled_blocks_out) {
	size_t cursor = 0;

	while (cursor < block_count) {
		size_t chunk = block_count - cursor;
		int blocks_read;
		size_t usable_blocks = 0;
		size_t skip_blocks = 0;
		size_t read_block;
		ssize_t written;

		if (chunk > BUFFER_SIZE) {
			chunk = BUFFER_SIZE;
		}

		read_block = segment_start + cursor;
		blocks_read = DVDReadBlocks(dvd_file, dvd_offset + (int)read_block, (int)chunk, buffer);
		if (blocks_read == (int)chunk) {
			usable_blocks = chunk;
		} else if (blocks_read > 0) {
			usable_blocks = (size_t)blocks_read;
			fprintf(stderr, _("Gap fill warning for %s: read %d of %zu blocks at %zu\n"),
				filename, blocks_read, chunk, read_block);
		} else {
			fprintf(stderr, _("Gap fill error for %s: read failure at block %zu\n"),
				filename, read_block);
		}

		if (usable_blocks > 0) {
			written = pwrite(fd, buffer, usable_blocks * DVD_VIDEO_LB_LEN,
					(off_t)read_block * DVD_VIDEO_LB_LEN);
			if (written != (ssize_t)(usable_blocks * DVD_VIDEO_LB_LEN)) {
				fprintf(stderr, _("Error writing %s during gap fill\n"), filename);
				perror(PACKAGE);
				return 1;
			}

			if (filled_blocks_out) {
				*filled_blocks_out += usable_blocks;
			}
		}

		if (usable_blocks < chunk) {
			size_t remaining = block_count - (cursor + usable_blocks);

			if (remaining == 0) {
				cursor = block_count;
				continue;
			}

			if (errorstrat == STRATEGY_ABORT) {
				return 1;
			} else if (errorstrat == STRATEGY_SKIP_BLOCK) {
				skip_blocks = 1;
				fprintf(stderr, _("Gap fill: skipping single block for %s\n"), filename);
			} else {
				size_t unread = chunk - usable_blocks;
				if (unread == 0) {
					unread = 1;
				}
				skip_blocks = unread;
				fprintf(stderr, _("Gap fill: skipping %zu blocks for %s\n"), skip_blocks, filename);
			}

			if (skip_blocks > remaining) {
				skip_blocks = remaining;
			}

			cursor += usable_blocks + skip_blocks;
		} else {
			cursor += chunk;
		}
	}

	return 0;
}


static int scan_existing_file_for_gaps(int fd, size_t expected_blocks, gap_plan_t* plan,
		size_t* blank_blocks_out, size_t* full_blocks_out, off_t* existing_bytes_out) {
	struct stat st;
	off_t existing_bytes;
	size_t full_blocks;
	size_t scan_blocks;
	unsigned char* buffer = NULL;
	size_t processed = 0;
	size_t blank_blocks = 0;
	size_t pending_start = SIZE_MAX;

	if (fstat(fd, &st) != 0) {
		return -1;
	}

	existing_bytes = st.st_size;
	if (existing_bytes_out) {
		*existing_bytes_out = existing_bytes;
	}

	full_blocks = (size_t)(existing_bytes / DVD_VIDEO_LB_LEN);
	scan_blocks = full_blocks;
	if (scan_blocks > expected_blocks) {
		scan_blocks = expected_blocks;
	}

	if (scan_blocks > 0) {
		buffer = (unsigned char*)malloc((size_t)BUFFER_SIZE * DVD_VIDEO_LB_LEN);
		if (buffer == NULL) {
			return -1;
		}
	}

	while (processed < scan_blocks) {
		size_t chunk_blocks = scan_blocks - processed;
		ssize_t bytes;
		size_t have_blocks;
		size_t i;

		if (chunk_blocks > BUFFER_SIZE) {
			chunk_blocks = BUFFER_SIZE;
		}

		bytes = pread(fd, buffer, (size_t)chunk_blocks * DVD_VIDEO_LB_LEN,
			(off_t)processed * DVD_VIDEO_LB_LEN);
		if (bytes < 0) {
			int saved_errno = errno;
			free(buffer);
			errno = saved_errno;
			return -1;
		}

		have_blocks = (size_t)bytes / DVD_VIDEO_LB_LEN;
		if (have_blocks == 0) {
			break;
		}
		if (have_blocks < chunk_blocks) {
			chunk_blocks = have_blocks;
		}

		for (i = 0; i < chunk_blocks; ++i) {
			size_t block_index = processed + i;
			const unsigned char* block_ptr = buffer + i * DVD_VIDEO_LB_LEN;

			if (buffer_is_blank(block_ptr, DVD_VIDEO_LB_LEN)) {
				if (pending_start == SIZE_MAX) {
					pending_start = block_index;
				}
			} else if (pending_start != SIZE_MAX) {
				size_t run = block_index - pending_start;
				if (gap_plan_add(plan, pending_start, run) != 0) {
					free(buffer);
					return -1;
				}
				blank_blocks += run;
				pending_start = SIZE_MAX;
			}
		}

		processed += chunk_blocks;
	}

	if (pending_start != SIZE_MAX) {
		size_t run = scan_blocks - pending_start;
		if (gap_plan_add(plan, pending_start, run) != 0) {
			free(buffer);
			return -1;
		}
		blank_blocks += run;
	}

	free(buffer);

	if (blank_blocks_out) {
		*blank_blocks_out = blank_blocks;
	}
	if (full_blocks_out) {
		*full_blocks_out = full_blocks;
	}

	return 0;
}


static size_t gap_collect_samples(const gap_plan_t* plan, size_t available_blocks,
		size_t desired, size_t samples[]) {
	size_t target;
	size_t count = 0;
	size_t i;

	if (available_blocks == 0 || desired == 0) {
		return 0;
	}

	target = desired;
	if (available_blocks < desired) {
		target = available_blocks;
	}

	for (i = 0; i < target; ++i) {
		size_t candidate = (size_t)(((uint64_t)(i + 1) * (uint64_t)available_blocks) / (target + 1));
		size_t forward = candidate;
		size_t backward;

		if (candidate >= available_blocks) {
			candidate = available_blocks - 1;
		}

		while (forward < available_blocks && gap_plan_contains(plan, forward)) {
			forward++;
		}
		if (forward >= available_blocks) {
			backward = candidate;
			while (backward > 0 && gap_plan_contains(plan, backward)) {
				backward--;
			}
			if (gap_plan_contains(plan, backward)) {
				continue;
			}
			forward = backward;
		}

		if (count > 0 && samples[count - 1] == forward) {
			continue;
		}

		samples[count++] = forward;
	}

	return count;
}


static int gap_verify_samples(int fd, dvd_file_t* dvd_file, int dvd_offset,
		const char* filename, const size_t samples[], size_t sample_count) {
	unsigned char dvd_block[DVD_VIDEO_LB_LEN];
	unsigned char file_block[DVD_VIDEO_LB_LEN];
	size_t i;
	ssize_t read_bytes;

	for (i = 0; i < sample_count; ++i) {
		size_t block = samples[i];

		if (DVDReadBlocks(dvd_file, dvd_offset + (int)block, 1, dvd_block) != 1) {
			fprintf(stderr, _("Error reading %s at block %zu during verification\n"), filename, block);
			return 1;
		}

		read_bytes = pread(fd, file_block, DVD_VIDEO_LB_LEN, (off_t)block * DVD_VIDEO_LB_LEN);
		if (read_bytes != DVD_VIDEO_LB_LEN) {
			fprintf(stderr, _("Error reading existing data from %s during verification\n"), filename);
			perror(PACKAGE);
			return 1;
		}

		if (memcmp(dvd_block, file_block, DVD_VIDEO_LB_LEN) != 0) {
			fprintf(stderr, _("Verification sample mismatch for %s at sector %zu\n"), filename, block);
			return 1;
		}
	}

	return 0;
}


static int gap_fill_from_plan(int fd, dvd_file_t* dvd_file, int dvd_offset,
		const gap_plan_t* plan, const char* filename,
		read_error_strategy_t errorstrat, size_t* filled_blocks_out) {
	unsigned char* buffer;
	size_t total_filled = 0;
	size_t range_index;
	int result = 0;

	if (plan->count == 0) {
		if (filled_blocks_out) {
			*filled_blocks_out = 0;
		}
		return 0;
	}

	buffer = (unsigned char*)malloc((size_t)BUFFER_SIZE * DVD_VIDEO_LB_LEN);
	if (buffer == NULL) {
		if (filled_blocks_out) {
			*filled_blocks_out = 0;
		}
		return 1;
	}

	if (gap_strategy == GAP_STRATEGY_RANDOM) {
		gap_fill_segment_t* segments = NULL;
		size_t segment_count = 0;
		size_t segment_capacity = 0;

		for (range_index = 0; range_index < plan->count; ++range_index) {
			size_t produced = 0;
			size_t range_start = plan->ranges[range_index].start_block;
			size_t range_blocks = plan->ranges[range_index].block_count;

			while (produced < range_blocks) {
				size_t chunk = range_blocks - produced;
				if (chunk > BUFFER_SIZE) {
					chunk = BUFFER_SIZE;
				}
				if (segment_count == segment_capacity) {
					size_t new_capacity = segment_capacity == 0 ? 32 : segment_capacity * 2;
					gap_fill_segment_t* new_segments = realloc(segments, new_capacity * sizeof(*new_segments));
					if (new_segments == NULL) {
						free(segments);
						free(buffer);
						if (filled_blocks_out) {
							*filled_blocks_out = total_filled;
						}
						return 1;
					}
					segments = new_segments;
					segment_capacity = new_capacity;
				}
				segments[segment_count].start_block = range_start + produced;
				segments[segment_count].block_count = chunk;
				segment_count++;
				produced += chunk;
			}
		}

		srand(gap_random_seed_set ? gap_random_seed : 0);
		for (size_t i = segment_count; i > 1; --i) {
			size_t j = (size_t)(rand() % (int)i);
			gap_fill_segment_t temp = segments[i - 1];
			segments[i - 1] = segments[j];
			segments[j] = temp;
		}

		for (size_t i = 0; i < segment_count; ++i) {
			if (gap_process_segment(fd, dvd_file, dvd_offset,
					segments[i].start_block, segments[i].block_count,
					filename, errorstrat, buffer, &total_filled) != 0) {
				result = 1;
				break;
			}
		}

		free(segments);
	} else {
		for (range_index = 0; range_index < plan->count && result == 0; ++range_index) {
			size_t range_start = plan->ranges[range_index].start_block;
			size_t range_blocks = plan->ranges[range_index].block_count;

			switch (gap_strategy) {
			case GAP_STRATEGY_FORWARD:
				if (gap_process_segment(fd, dvd_file, dvd_offset,
							range_start, range_blocks, filename,
							errorstrat, buffer, &total_filled) != 0) {
					result = 1;
				}
				break;

			case GAP_STRATEGY_REVERSE: {
				size_t processed = 0;
				while (processed < range_blocks && result == 0) {
					size_t chunk = range_blocks - processed;
					if (chunk > BUFFER_SIZE) {
						chunk = BUFFER_SIZE;
					}
					size_t segment_start = range_start + range_blocks - processed - chunk;
					if (gap_process_segment(fd, dvd_file, dvd_offset,
								segment_start, chunk, filename,
								errorstrat, buffer, &total_filled) != 0) {
						result = 1;
						break;
					}
					processed += chunk;
				}
				break;
			}

			case GAP_STRATEGY_OUTSIDE_IN: {
				size_t front = 0;
				size_t back = range_blocks;
				int use_front = 1;
				while (front < back && result == 0) {
					size_t remaining = back - front;
					size_t chunk = remaining;
					if (chunk > BUFFER_SIZE) {
						chunk = BUFFER_SIZE;
					}

					if (use_front) {
						size_t segment_start = range_start + front;
						if (gap_process_segment(fd, dvd_file, dvd_offset,
									segment_start, chunk, filename,
									errorstrat, buffer, &total_filled) != 0) {
							result = 1;
							break;
						}
						front += chunk;
					} else {
						size_t segment_start = range_start + (back - chunk);
						if (gap_process_segment(fd, dvd_file, dvd_offset,
									segment_start, chunk, filename,
									errorstrat, buffer, &total_filled) != 0) {
							result = 1;
							break;
						}
						back -= chunk;
					}

					use_front = !use_front;
				}
				break;
			}

			case GAP_STRATEGY_RANDOM:
				/* handled above */
				break;
			}
		}
	}

	free(buffer);
	if (filled_blocks_out) {
		*filled_blocks_out = total_filled;
	}
	return result;
}


static void gap_print_report(const char* path, size_t expected_blocks,
		size_t blank_before, size_t truncated_before,
		size_t blank_after, size_t truncated_after,
		size_t filled_blocks) {
	double blank_pct_before = 0.0;
	double trunc_pct_before = 0.0;
	double blank_pct_after = 0.0;
	double trunc_pct_after = 0.0;

	if (expected_blocks > 0) {
		blank_pct_before = (double)blank_before * 100.0 / (double)expected_blocks;
		trunc_pct_before = (double)truncated_before * 100.0 / (double)expected_blocks;
		blank_pct_after = (double)blank_after * 100.0 / (double)expected_blocks;
		trunc_pct_after = (double)truncated_after * 100.0 / (double)expected_blocks;
	}

	printf(_("Gaps report for %s: filled %zu sectors; before %zu zeroed (%.2f%%), %zu missing (%.2f%%); after %zu zeroed (%.2f%%), %zu missing (%.2f%%)\n"),
		path, filled_blocks,
		blank_before, blank_pct_before, truncated_before, trunc_pct_before,
		blank_after, blank_pct_after, truncated_after, trunc_pct_after);
}


static int DVDCmpBlocks(dvd_file_t* dvd_file, int fd, int offset, int size,
		const char* path, const char* label, read_error_strategy_t errorstrat) {
	unsigned char dvd_buffer[BUFFER_SIZE * DVD_VIDEO_LB_LEN];
	unsigned char file_buffer[BUFFER_SIZE * DVD_VIDEO_LB_LEN];
	int remaining = size;
	int total = size;
	int to_read = BUFFER_SIZE;
	int current_offset = offset;
	size_t compared_blocks = 0;

	(void)errorstrat;

	if (lseek(fd, (off_t)offset * DVD_VIDEO_LB_LEN, SEEK_SET) == (off_t)-1) {
		perror(PACKAGE);
		return 1;
	}

	while (remaining > 0) {
		if (to_read > remaining) {
			to_read = remaining;
		}

		int act_read = DVDReadBlocks(dvd_file, current_offset, to_read, dvd_buffer);
		if (act_read != to_read) {
			if (progress) {
				fprintf(stdout, "\n");
			}
			if (act_read >= 0) {
				fprintf(stderr, _("Error reading %s at block %d\n"), label, current_offset + act_read);
			} else {
				fprintf(stderr, _("Error reading %s at block %d, read error returned\n"), label, current_offset);
			}
			return 1;
		}

		size_t chunk_bytes = (size_t)act_read * DVD_VIDEO_LB_LEN;
		size_t total_read = 0;
		while (total_read < chunk_bytes) {
			ssize_t got = read(fd, file_buffer + total_read, chunk_bytes - total_read);
			if (got < 0) {
				if (errno == EINTR) {
					continue;
				}
				perror(PACKAGE);
				return 1;
			}
			if (got == 0) {
				fprintf(stderr, _("File %s ended prematurely while comparing\n"), path);
				return 1;
			}
			total_read += (size_t)got;
		}

		if (memcmp(dvd_buffer, file_buffer, chunk_bytes) != 0) {
			size_t block_index;
			for (block_index = 0; block_index < (size_t)act_read; ++block_index) {
				if (memcmp(dvd_buffer + block_index * DVD_VIDEO_LB_LEN,
					file_buffer + block_index * DVD_VIDEO_LB_LEN,
					DVD_VIDEO_LB_LEN) != 0) {
					fprintf(stderr, _("Data mismatch for %s at sector %lld\n"),
						path, (long long)(current_offset + (int)block_index));
					break;
				}
			}
			return 1;
		}

		current_offset += act_read;
		remaining -= act_read;
		compared_blocks += (size_t)act_read;

		if (progress) {
			int done = (int)compared_blocks;
			if (remaining < BUFFER_SIZE || (done % BUFFER_SIZE) == 0) {
				float doneMiB = (float)done / 512.0f;
				float totalMiB = (float)total / 512.0f;
				fprintf(stdout, "\r");
				fprintf(stdout, _("Comparing %s: %.0f%% done (%.0f/%.0f MiB)"),
						progressText, doneMiB / totalMiB * 100.0f, doneMiB, totalMiB);
				fflush(stdout);
			}
		}
	}

	unsigned char extra;
	ssize_t extra_read = read(fd, &extra, 1);
	if (extra_read < 0) {
		perror(PACKAGE);
		return 1;
	} else if (extra_read > 0) {
		fprintf(stderr, _("File %s contains extra data beyond expected size\n"), path);
		return 1;
	}

	if (progress) {
		fprintf(stdout, "\n");
	}

	return 0;
}


static int CheckSizeArray(const int size_array[], int reference, int target) {
	if(size_array[target] && (size_array[reference]/size_array[target] == 1) &&
			((size_array[reference] * 2 - size_array[target])/ size_array[target] == 1) &&
			((size_array[reference]%size_array[target] * 3) < size_array[reference]) ) {
		/* We have a dual DVD with two feature films - now let's see if they have the same amount of chapters*/
		return(1);
	} else {
		return(0);
	}
}


static int CheckAudioSubChannels(int audio_audio_array[], int title_set_audio_array[],
		int subpicture_sub_array[], int title_set_sub_array[],
		int channels_channel_array[],int title_set_channel_array[],
		int reference, int candidate, int title_sets) {

	int temp, i, found_audio, found_sub, found_channels;

	found_audio=0;
	temp = audio_audio_array[reference];
	for (i=0 ; i < title_sets ; i++ ) {
		if ( audio_audio_array[i] < temp ) {
			break;
		}
		if ( candidate == title_set_audio_array[i] ) {
			found_audio=1;
			break;
		}

	}

	found_sub=0;
	temp = subpicture_sub_array[reference];
	for (i=0 ; i < title_sets ; i++ ) {
		if ( subpicture_sub_array[i] < temp ) {
			break;
		}
		if ( candidate == title_set_sub_array[i] ) {
			found_sub=1;
			break;
		}

	}


	found_channels=0;
	temp = channels_channel_array[reference];
	for (i=0 ; i < title_sets ; i++ ) {
		if ( channels_channel_array[i] < temp ) {
			break;
		}
		if ( candidate == title_set_channel_array[i] ) {
			found_channels=1;
			break;
		}

	}


	return(found_audio + found_sub + found_channels);
}




static int DVDWriteCells(dvd_reader_t * dvd, int cell_start_sector[],
		int cell_end_sector[], int length, int titles,
		title_set_info_t * title_set_info, titles_info_t * titles_info,
		char * targetdir,char * title_name) {

	/* Loop variables */
	int i;

	/* Vob control */
	int vob = 1;

	/* Temp filename,dirname */
	char *targetname = NULL;
	size_t targetname_length;

	/* Write buffers */
	unsigned char *buffer = NULL;
	unsigned char *existing_buffer = NULL;

	/* File Handler */
	int streamout = -1;

	int size;
	int left;
	int to_read;
	int have_read;
	int soffset;

	/* DVD handler */
	dvd_file_t* dvd_file = NULL;

	int title_set;
	int result = 1;
	int open_flags;
	size_t vob_total_blocks = 0;
	size_t vob_blank_before = 0;
	size_t vob_blank_after = 0;

#ifdef DEBUG
	int number_of_vob_files;
	fprintf(stderr,"DVDWriteCells: length is %d\n", length);
#endif

	title_set = titles_info->titles[titles - 1].title_set;
#ifdef DEBUG
	number_of_vob_files = title_set_info->title_set[title_set].number_of_vob_files;
	fprintf(stderr,"DVDWriteCells: title set is %d\n", title_set);
	fprintf(stderr,"DVDWriteCells: vob files are %d\n", number_of_vob_files);
#endif

	// Reserve space for "<targetdir>/<title_name>/VIDEO_TS/VTS_XX_X.VOB" and terminating "\0"
	targetname_length = strlen(targetdir) + strlen(title_name) + 24;
	targetname = malloc(targetname_length);
	if (targetname == NULL) {
		fprintf(stderr, _("Failed to allocate %zu bytes for a filename.\n"), targetname_length);
		goto cleanup;
	}

	/* Remove all old files silently if they exists */
	if (!fill_gaps) {
		for (i = 0; i < 10; i++) {
			snprintf(targetname, targetname_length, "%s/%s/VIDEO_TS/VTS_%02i_%i.VOB", targetdir, title_name, title_set, i + 1);
#ifdef DEBUG
			fprintf(stderr,"DVDWriteCells: file is %s\n", targetname);
#endif
			unlink(targetname);
		}
	}

#ifdef DEBUG
	for (i = 0; i < number_of_vob_files; i++) {
		fprintf(stderr,"vob %i size: %lld\n", i + 1, title_set_info->title_set[title_set].size_vob[i]);
	}
#endif

	/* Create VTS_XX_X.VOB */
	if (title_set == 0) {
		fprintf(stderr,_("Do not try to copy chapters from the VMG domain; there are none.\n"));
		goto cleanup;
	} else {
		snprintf(targetname, targetname_length, "%s/%s/VIDEO_TS/VTS_%02i_%i.VOB", targetdir, title_name, title_set, vob);
	}

#ifdef DEBUG
	fprintf(stderr,"DVDWriteCells: 1\n");
#endif

	buffer = (unsigned char *)malloc(BUFFER_SIZE * DVD_VIDEO_LB_LEN * sizeof(unsigned char));
	if (buffer == NULL) {
		fprintf(stderr, _("Out of memory copying %s\n"), targetname);
		goto cleanup;
	}

#ifdef DEBUG
	fprintf(stderr,"DVDWriteCells: 2\n");
#endif

	if (fill_gaps) {
		existing_buffer = (unsigned char *)malloc(BUFFER_SIZE * DVD_VIDEO_LB_LEN * sizeof(unsigned char));
		if (existing_buffer == NULL) {
			fprintf(stderr, _("Out of memory copying %s\n"), targetname);
			goto cleanup;
		}
	}

	open_flags = fill_gaps ? (O_RDWR | O_CREAT) : (O_WRONLY | O_CREAT | O_APPEND);
	streamout = open(targetname, open_flags, 0666);
	if (streamout == -1) {
		fprintf(stderr, _("Error creating %s\n"), targetname);
		perror(PACKAGE);
		goto cleanup;
	}

#ifdef DEBUG
	fprintf(stderr,"DVDWriteCells: 3\n");
#endif

	dvd_file = DVDOpenFile(dvd, title_set, DVD_READ_TITLE_VOBS);
	if (dvd_file == 0) {
		fprintf(stderr, _("Failed opening TITLE VOB\n"));
		goto cleanup;
	}

	size = 0;

	for (i = 0; i < length; i++) {
		left = cell_end_sector[i] - cell_start_sector[i];
		soffset = cell_start_sector[i];

		while (left > 0) {
			to_read = left;
			if (to_read + size > MAX_VOB_SIZE) {
				to_read = MAX_VOB_SIZE - size;
			}
			if (to_read > BUFFER_SIZE) {
				to_read = BUFFER_SIZE;
			}

			have_read = DVDReadBlocks(dvd_file, soffset, to_read, buffer);
			if (have_read < 0) {
				fprintf(stderr, _("Error reading TITLE VOB: %d != %d\n"), have_read, to_read);
				result = 1;
				goto cleanup;
			}
			if (have_read == 0) {
				fprintf(stderr, _("Error reading TITLE VOB: no data read\n"));
				result = 1;
				goto cleanup;
			}
			if (have_read < to_read) {
				fprintf(stderr, _("DVDReadBlocks read %d blocks of %d blocks\n"), have_read, to_read);
			}

		if (fill_gaps) {
			size_t chunk_blocks = (size_t)have_read;
			size_t chunk_bytes = chunk_blocks * DVD_VIDEO_LB_LEN;
			off_t chunk_offset = (off_t)size * DVD_VIDEO_LB_LEN;
			ssize_t existing_bytes = read_existing_range(streamout, chunk_offset, existing_buffer, chunk_bytes);
			if (existing_bytes < 0) {
					fprintf(stderr, _("Error reading existing data from %s\n"), targetname);
					perror(PACKAGE);
					result = 1;
					goto cleanup;
				}

				size_t block_size = DVD_VIDEO_LB_LEN;
				size_t existing_blocks = (size_t)existing_bytes / block_size;
				size_t partial_bytes = (size_t)existing_bytes % block_size;
				size_t pending_start = SIZE_MAX;

				for (size_t block_idx = 0; block_idx < chunk_blocks; ++block_idx) {
					unsigned char* existing_block = existing_buffer + block_idx * block_size;
					const unsigned char* dvd_block = buffer + block_idx * block_size;
					int block_has_full = (block_idx < existing_blocks);
					int block_has_partial = (!block_has_full && (block_idx == existing_blocks) && (partial_bytes > 0));
					int block_blank;
					int after_blank = buffer_is_blank(dvd_block, block_size);

					if (block_has_full) {
						block_blank = buffer_is_blank(existing_block, block_size);
						if (!block_blank) {
							if (memcmp(existing_block, dvd_block, block_size) != 0) {
								fprintf(stderr, _("Existing data in %s does not match the DVD at offset %lld\n"), targetname, (long long)(chunk_offset + (off_t)block_idx * block_size));
								result = 1;
								goto cleanup;
							}
						}
					} else if (block_has_partial) {
						block_blank = buffer_is_blank(existing_block, partial_bytes);
						if (!block_blank) {
							if (memcmp(existing_block, dvd_block, partial_bytes) != 0) {
								fprintf(stderr, _("Existing data in %s does not match the DVD at offset %lld\n"), targetname, (long long)(chunk_offset + (off_t)block_idx * block_size));
								result = 1;
								goto cleanup;
							}
						}
					} else {
						block_blank = 1;
					}

					vob_total_blocks++;
					if (block_blank) {
						vob_blank_before++;
					}
					if (after_blank) {
						vob_blank_after++;
					}

					if (block_blank) {
						if (pending_start == SIZE_MAX) {
							pending_start = block_idx;
						}
					} else if (pending_start != SIZE_MAX) {
						size_t pending_blocks = block_idx - pending_start;
						off_t write_offset = chunk_offset + (off_t)pending_start * block_size;
						size_t bytes_to_write = pending_blocks * block_size;
						if (write_range(streamout, write_offset, buffer + pending_start * block_size, bytes_to_write) != 0) {
							fprintf(stderr, _("Error writing TITLE VOB\n"));
							perror(PACKAGE);
							result = 1;
							goto cleanup;
						}
						pending_start = SIZE_MAX;
					}
				}

				if (pending_start != SIZE_MAX) {
					size_t pending_blocks = chunk_blocks - pending_start;
					off_t write_offset = chunk_offset + (off_t)pending_start * block_size;
					size_t bytes_to_write = pending_blocks * block_size;
					if (write_range(streamout, write_offset, buffer + pending_start * block_size, bytes_to_write) != 0) {
						fprintf(stderr, _("Error writing TITLE VOB\n"));
						perror(PACKAGE);
						result = 1;
						goto cleanup;
					}
				}
			} else {
				if (write(streamout, buffer, have_read * DVD_VIDEO_LB_LEN) != have_read * DVD_VIDEO_LB_LEN) {
					fprintf(stderr, _("Error writing TITLE VOB\n"));
					perror(PACKAGE);
					result = 1;
					goto cleanup;
				}
			}


#ifdef DEBUG
			fprintf(stderr,"Current soffset changed from %i to ",soffset);
#endif
			soffset = soffset + have_read;
#ifdef DEBUG
			fprintf(stderr,"%i\n",soffset);
#endif
			left -= have_read;
			size += have_read;

			if ((size >= MAX_VOB_SIZE) && (left > 0)) {
#ifdef DEBUG
				fprintf(stderr,"size: %i, MAX_VOB_SIZE: %i\n ",size, MAX_VOB_SIZE);
#endif
				if (finalize_vob_file(streamout, targetname, (size_t)size,
						vob_total_blocks, vob_blank_before, vob_blank_after) != 0) {
					result = 1;
					goto cleanup;
				}
				close(streamout);
				streamout = -1;
				vob = vob + 1;
				size = 0;
				vob_total_blocks = 0;
				vob_blank_before = 0;
				vob_blank_after = 0;
				snprintf(targetname, targetname_length, "%s/%s/VIDEO_TS/VTS_%02i_%i.VOB", targetdir, title_name, title_set, vob);
				streamout = open(targetname, open_flags, 0666);
				if (streamout == -1) {
					fprintf(stderr, _("Error creating %s\n"), targetname);
					perror(PACKAGE);
					result = 1;
					goto cleanup;
				}
			}
		}
	}

	if (finalize_vob_file(streamout, targetname, (size_t)size,
			vob_total_blocks, vob_blank_before, vob_blank_after) != 0) {
		result = 1;
		goto cleanup;
	}

	result = 0;

cleanup:
	if (dvd_file) {
		DVDCloseFile(dvd_file);
	}
	if (streamout != -1) {
		close(streamout);
	}
	free(existing_buffer);
	free(buffer);
	free(targetname);

	return result;
}



static void FreeSortArrays( int chapter_chapter_array[], int title_set_chapter_array[],
		int angle_angle_array[], int title_set_angle_array[],
		int subpicture_sub_array[], int title_set_sub_array[],
		int audio_audio_array[], int title_set_audio_array[],
		int size_size_array[], int title_set_size_array[],
		int channels_channel_array[], int title_set_channel_array[]) {


	free(chapter_chapter_array);
	free(title_set_chapter_array);

	free(angle_angle_array);
	free(title_set_angle_array);

	free(subpicture_sub_array);
	free(title_set_sub_array);

	free(audio_audio_array);
	free(title_set_audio_array);

	free(size_size_array);
	free(title_set_size_array);

	free(channels_channel_array);
	free(title_set_channel_array);
}


static titles_info_t * DVDGetInfo(dvd_reader_t * _dvd) {

	/* title interation */
	int counter, i, f;

	/* Our guess */
	int candidate;
	int multi = 0;
	int dual = 0;


	int titles;
	int title_sets;

	/* Arrays for chapter, angle, subpicture, audio, size, aspect, channels - file_set relationship */

	/* Size == number_of_titles */
	int * chapter_chapter_array;
	int * title_set_chapter_array;

	int * angle_angle_array;
	int * title_set_angle_array;

	/* Size == number_of_title_sets */

	int * subpicture_sub_array;
	int * title_set_sub_array;

	int * audio_audio_array;
	int * title_set_audio_array;

	int * size_size_array;
	int * title_set_size_array;

	int * channels_channel_array;
	int * title_set_channel_array;

	/* Temp helpers */
	int channels;
	int found;
	int chapters_1;
	int chapters_2;
	int found_chapter;
	int number_of_multi;


	/* DVD handlers */
	ifo_handle_t* vmg_ifo = NULL;
	dvd_file_t* vts_title_file = NULL;

	titles_info_t* titles_info = NULL;

	/* Open main info file */
	vmg_ifo = ifoOpen( _dvd, 0 );
	if(!vmg_ifo) {
		fprintf( stderr, _("Cannot open VMG info.\n"));
		return (0);
	}

	titles = vmg_ifo->tt_srpt->nr_of_srpts;
	title_sets = vmg_ifo->vmgi_mat->vmg_nr_of_title_sets;

	if ((vmg_ifo->tt_srpt == 0) || (vmg_ifo->vts_atrt == 0)) {
		ifoClose(vmg_ifo);
		return(0);
	}


	if((titles_info = (titles_info_t *)malloc(sizeof(titles_info_t))) == NULL) {
		fprintf(stderr, _("Out of memory creating titles info structure\n"));
		return NULL;
	}

	titles_info->titles = (titles_t *)malloc((titles)* sizeof(titles_t));
	titles_info->number_of_titles = titles;


	chapter_chapter_array = malloc(titles * sizeof(int));
	title_set_chapter_array = malloc(titles * sizeof(int));

	/*currently not used in the guessing */
	angle_angle_array = malloc(titles * sizeof(int));
	title_set_angle_array = malloc(titles * sizeof(int));


	subpicture_sub_array = malloc(title_sets * sizeof(int));
	title_set_sub_array = malloc(title_sets * sizeof(int));

	audio_audio_array = malloc(title_sets * sizeof(int));
	title_set_audio_array = malloc(title_sets * sizeof(int));

	size_size_array = malloc(title_sets * sizeof(int));
	title_set_size_array = malloc(title_sets * sizeof(int));

	channels_channel_array = malloc(title_sets * sizeof(int));
	title_set_channel_array = malloc(title_sets * sizeof(int));

	/* Check mallocs */
	if(!titles_info->titles || !chapter_chapter_array || !title_set_chapter_array ||
			!angle_angle_array || !title_set_angle_array || !subpicture_sub_array ||
			!title_set_sub_array || !audio_audio_array || !title_set_audio_array ||
			!size_size_array || !title_set_size_array || !channels_channel_array ||
			!title_set_channel_array) {
		fprintf(stderr, _("Out of memory creating arrays for titles info\n"));
		free(titles_info);
		FreeSortArrays( chapter_chapter_array, title_set_chapter_array,
						angle_angle_array, title_set_angle_array,
						subpicture_sub_array, title_set_sub_array,
						audio_audio_array, title_set_audio_array,
						size_size_array, title_set_size_array,
						channels_channel_array, title_set_channel_array);
		return NULL;
	}

	/* Interate over the titles nr_of_srpts */
	for(counter = 0; counter < titles; counter++) {
		/* For titles_info */
		titles_info->titles[counter].title = counter + 1;
		titles_info->titles[counter].title_set = vmg_ifo->tt_srpt->title[counter].title_set_nr;
		titles_info->titles[counter].vts_title = vmg_ifo->tt_srpt->title[counter].vts_ttn;
		titles_info->titles[counter].chapters = vmg_ifo->tt_srpt->title[counter].nr_of_ptts;
		titles_info->titles[counter].angles = vmg_ifo->tt_srpt->title[counter].nr_of_angles;

		/* For main title*/
		chapter_chapter_array[counter] = vmg_ifo->tt_srpt->title[counter].nr_of_ptts;
		title_set_chapter_array[counter] = vmg_ifo->tt_srpt->title[counter].title_set_nr;
		angle_angle_array[counter] = vmg_ifo->tt_srpt->title[counter].nr_of_angles;
		title_set_angle_array[counter] = vmg_ifo->tt_srpt->title[counter].title_set_nr;
	}

	/* Interate over vmg_nr_of_title_sets */

	for(counter = 0; counter < title_sets; counter++) {

		/* Picture*/
		subpicture_sub_array[counter] = vmg_ifo->vts_atrt->vts[counter].nr_of_vtstt_subp_streams;
		title_set_sub_array[counter] = counter + 1;


		/* Audio */
		audio_audio_array[counter] = vmg_ifo->vts_atrt->vts[counter].nr_of_vtstt_audio_streams;
		title_set_audio_array[counter] = counter + 1;

		channels=0;
		for(i = 0; i < audio_audio_array[counter]; i++) {
			if ( channels < vmg_ifo->vts_atrt->vts[counter].vtstt_audio_attr[i].channels + 1) {
				channels = vmg_ifo->vts_atrt->vts[counter].vtstt_audio_attr[i].channels + 1;
			}

		}
		channels_channel_array[counter] = channels;
		title_set_channel_array[counter] = counter + 1;

		/* For titles_info */
		for (f=0; f < titles_info->number_of_titles ; f++ ) {
			if ( titles_info->titles[f].title_set == counter + 1 ) {
				titles_info->titles[f].aspect_ratio = vmg_ifo->vts_atrt->vts[counter].vtstt_vobs_video_attr.display_aspect_ratio;
				titles_info->titles[f].sub_pictures = vmg_ifo->vts_atrt->vts[counter].nr_of_vtstt_subp_streams;
				titles_info->titles[f].audio_tracks = vmg_ifo->vts_atrt->vts[counter].nr_of_vtstt_audio_streams;
				titles_info->titles[f].audio_channels = channels;
			}
		}

	}




	for (counter=0; counter < title_sets; counter++ ) {

		vts_title_file = DVDOpenFile(_dvd, counter + 1, DVD_READ_TITLE_VOBS);

		if(vts_title_file != 0) {
			size_size_array[counter] = DVDFileSize(vts_title_file);
			DVDCloseFile(vts_title_file);
		} else {
			size_size_array[counter] = 0;
		}

		title_set_size_array[counter] = counter + 1;


	}


	/* Sort all arrays max to min */

	bsort_max_to_min(chapter_chapter_array, title_set_chapter_array, titles);
	bsort_max_to_min(angle_angle_array, title_set_angle_array, titles);
	bsort_max_to_min(subpicture_sub_array, title_set_sub_array, title_sets);
	bsort_max_to_min(audio_audio_array, title_set_audio_array, title_sets);
	bsort_max_to_min(size_size_array, title_set_size_array, title_sets);
	bsort_max_to_min(channels_channel_array, title_set_channel_array, title_sets);


	/* Check if the second biggest one actually can be a feature title */
	/* Here we will take do biggest/second and if that is bigger than one it's not a feauture title */
	/* Now this is simply not enough since we have to check that the diff between the two of them is small enough
	 to consider the second one a feature title we are doing two checks (biggest + biggest - second) /second == 1
	 and biggest%second * 3 < biggest */

	if ( title_sets > 1 && CheckSizeArray(size_size_array, 0, 1) == 1 ) {
		/* We have a dual DVD with two feature films - now let's see if they have the same amount of chapters*/

		chapters_1 = 0;
		for (i=0 ; i < titles ; i++ ) {
			if (titles_info->titles[i].title_set == title_set_size_array[0] ) {
				if ( chapters_1 < titles_info->titles[i].chapters){
					chapters_1 = titles_info->titles[i].chapters;
				}
			}
		}

		chapters_2 = 0;
		for (i=0 ; i < titles ; i++ ) {
			if (titles_info->titles[i].title_set == title_set_size_array[1] ) {
				if ( chapters_2 < titles_info->titles[i].chapters){
					chapters_2 = titles_info->titles[i].chapters;
				}
			}
		}

		if(vmg_ifo->vts_atrt->vts[title_set_size_array[0] - 1].vtstt_vobs_video_attr.display_aspect_ratio ==
			vmg_ifo->vts_atrt->vts[title_set_size_array[1] - 1].vtstt_vobs_video_attr.display_aspect_ratio) {
			/* In this case it's most likely so that we have a dual film but with different context
			They are with in the same size range and have the same aspect ratio
			I would guess that such a case is e.g. a DVD containing several episodes of a TV serie*/
			candidate = title_set_size_array[0];
			multi = 1;
		} else if ( chapters_1 == chapters_2 && vmg_ifo->vts_atrt->vts[title_set_size_array[0] - 1].vtstt_vobs_video_attr.display_aspect_ratio !=
			vmg_ifo->vts_atrt->vts[title_set_size_array[1] - 1].vtstt_vobs_video_attr.display_aspect_ratio){
			/* In this case we have (guess only) the same context - they have the same number of chapters but different aspect ratio and are in the same size range*/
			if ( vmg_ifo->vts_atrt->vts[title_set_size_array[0] - 1].vtstt_vobs_video_attr.display_aspect_ratio == aspect) {
				candidate = title_set_size_array[0];
			} else if ( vmg_ifo->vts_atrt->vts[title_set_size_array[1] - 1].vtstt_vobs_video_attr.display_aspect_ratio == aspect) {
				candidate = title_set_size_array[1];
			} else {
				/* Okay we didn't have the prefered aspect ratio - just make the biggest one a candidate */
				/* please send report if this happens*/
				fprintf(stderr, _("You have encountered a very special DVD; please send a bug report along with all IFO files from this title\n"));
				candidate = title_set_size_array[0];
			}
			dual = 1;
		} else {
			/* Can this case ever happen? */
			candidate = title_set_size_array[0];
		}
	} else {
		candidate = title_set_size_array[0];
	}


	/* Lets start checking audio,sub pictures and channels my guess is namly that a special suburb will put titles with a lot of
	 chapters just to make our backup hard */


	found = CheckAudioSubChannels(audio_audio_array, title_set_audio_array,
			subpicture_sub_array, title_set_sub_array,
			channels_channel_array, title_set_channel_array,
			0 , candidate, title_sets);


	/* Now let's see if we can find our candidate among the top most chapters */
	found_chapter=6;
	for (i=0 ; (i < titles) && (i < 4) ; i++ ) {
		if ( candidate == title_set_chapter_array[i] ) {
			found_chapter=i+1;
			break;
		}
	}

	/* Close the VMG ifo file we got all the info we need */
	ifoClose(vmg_ifo);


	if (((found == 3) && (found_chapter == 1) && (dual == 0) && (multi == 0)) || ((found == 3) && (found_chapter < 3 ) && (dual == 1))) {

		FreeSortArrays( chapter_chapter_array, title_set_chapter_array,
				angle_angle_array, title_set_angle_array,
				subpicture_sub_array, title_set_sub_array,
				audio_audio_array, title_set_audio_array,
				size_size_array, title_set_size_array,
				channels_channel_array, title_set_channel_array);
		titles_info->main_title_set = candidate;
		return(titles_info);

	}

	if (multi == 1) {
		for (i=0 ; i < title_sets ; ++i) {
			if (CheckSizeArray(size_size_array, 0, i + 1) == 0) {
				break;
			}
		}
		number_of_multi = i;
		for (i = 0; i < number_of_multi; i++ ) {
			if (title_set_chapter_array[0] == i + 1) {
				candidate = title_set_chapter_array[0];
			}
		}

		found = CheckAudioSubChannels(audio_audio_array, title_set_audio_array,
				subpicture_sub_array, title_set_sub_array,
				channels_channel_array, title_set_channel_array,
				0 , candidate, title_sets);

		if (found == 3) {
			FreeSortArrays( chapter_chapter_array, title_set_chapter_array,
					angle_angle_array, title_set_angle_array,
					subpicture_sub_array, title_set_sub_array,
					audio_audio_array, title_set_audio_array,
					size_size_array, title_set_size_array,
					channels_channel_array, title_set_channel_array);
			titles_info->main_title_set = candidate;
			return(titles_info);
		}
	}

	/* We have now come to that state that we more or less have given up :( giving you a good guess of the main feature film*/
	/*No matter what we will more or less only return the biggest VOB*/
	/* Lets see if we can find our biggest one - then we return that one */
	candidate = title_set_size_array[0];

	found = CheckAudioSubChannels(audio_audio_array, title_set_audio_array,
			subpicture_sub_array, title_set_sub_array,
			channels_channel_array, title_set_channel_array,
			0 , candidate, title_sets);

	/* Now let's see if we can find our candidate among the top most chapters */

	found_chapter=5;
	for (i=0 ; (i < titles) && (i < 4) ; i++ ) {
		if ( candidate == title_set_chapter_array[i] ) {
			found_chapter=i+1;
			break;
		}

	}

	/* Here we take chapters in to consideration*/
	if (found == 3) {
		FreeSortArrays( chapter_chapter_array, title_set_chapter_array,
				angle_angle_array, title_set_angle_array,
				subpicture_sub_array, title_set_sub_array,
				audio_audio_array, title_set_audio_array,
				size_size_array, title_set_size_array,
				channels_channel_array, title_set_channel_array);
		titles_info->main_title_set = candidate;
		return(titles_info);
	}

	/* Here we do but we lower the treshold for audio, sub and channels */

	if ((found > 1 ) && (found_chapter <= 4)) {
		FreeSortArrays( chapter_chapter_array, title_set_chapter_array,
				angle_angle_array, title_set_angle_array,
				subpicture_sub_array, title_set_sub_array,
				audio_audio_array, title_set_audio_array,
				size_size_array, title_set_size_array,
				channels_channel_array, title_set_channel_array);
		titles_info->main_title_set = candidate;
		return(titles_info);

		/* return it */
	} else {
		/* Here we give up and just return the biggest one :(*/
		/* Just return the biggest badest one*/
		FreeSortArrays( chapter_chapter_array, title_set_chapter_array,
				angle_angle_array, title_set_angle_array,
				subpicture_sub_array, title_set_sub_array,
				audio_audio_array, title_set_audio_array,
				size_size_array, title_set_size_array,
				channels_channel_array, title_set_channel_array);
		titles_info->main_title_set = candidate;
		return(titles_info);
	}


	/* Some radom thoughts about DVD guessing */
	/* We will now gather as much data about the DVD-Video as we can and
	then make a educated guess which one is the main feature film of it*/


	/* Make a tripple array with chapters, angles and title sets
	 - sort out dual title sets with a low number of chapters. Tradtionaly
	 the title set with most chapters is the main film. Number of angles is
	 keept as a reference point of low value*/

	/* Make a dual array with number of audio streams, sub picture streams
	 and title sets. Tradtionaly the main film has many audio streams
	 since it's supposed be synchronised e.g. a English film synchronised/dubbed
	 in German. We are also keeping track of sub titles since it's also indication
	 of the main film*/

	/* Which title set is the biggest one - dual array with title sets and size
	 The biggest one is usally the main film*/

	/* Which title set is belonging to title 1 and how many chapters has it. Once
	 again tradtionaly title one is belonging to the main film*/

	/* Yes a lot of rant - but it helps me think - some sketch on paper or in the mind
	 I sketch in the comments - beside it will help you understand the code*/

	/* Okay let's see if the biggest one has most chapters, it also has more subtitles
	 and audio tracks than the second one and it's title one.
	 Done it must be the main film

	 Hmm the biggest one doesn't have the most chapters?

	 See if the second one has the same amount of chapters and is the biggest one
	 If so we probably have a 4:3 and 16:9 versions of film on the same disk

	 Now we fetch the 16:9 by default unless the forced to do 4:3
	 First check which one is which.
	 If the 16:9 is the biggest one and has the same or more subtitle, audio streams
	 then we are happy unless we are in force 4:3 mode :(
	 The same goes in reverse if we are in force 4:3 mode


	 Hmm, in force 4:3 mode - now we check how much smaller than the biggest one it is
	 (or the reverse if we are in 16:9 mode)

	 Generally a reverse division should render in 1 and with a small modulo - like wise
	 a normal modulo should give us a high modulo

	 If we get more than one it's of cource a fake however if we get just one we still need to check
	 if we subtract the smaller one from the bigger one we should end up with a small number - hence we
	 need to multiply it more than 4 times to get it bigger than the biggest one. Now we know that the
	 two biggest once are really big and possibly carry the same film in differnet formats.

	 We will now return the prefered one either 16:9 or 4:3 but we will first check that the one
	 we return at lest has two or more audio tracks. We don't want it if the other one has a lot
	 more sound (we may end up with a film that only has 2ch Dolby Digital so we want to check for
	 6ch DTS or Dolby Digital. If the prefered one doesn't have those features but the other once has
	 we will return the other one.
	 */

}


static int DVDCopyBlocksFillGaps(dvd_file_t* dvd_file, int destination, int offset,
		int size, const char* path, const char* label, read_error_strategy_t errorstrat) {
	gap_plan_t plan = {0};
	size_t blank_blocks = 0;
	size_t existing_blocks = 0;
	off_t existing_bytes = 0;
	size_t truncated_blocks = 0;
	size_t sample_slots[GAP_SAMPLE_TARGET];
	size_t sample_count;
	int result = 0;

	if (scan_existing_file_for_gaps(destination, (size_t)size, &plan,
			&blank_blocks, &existing_blocks, &existing_bytes) != 0) {
		perror(PACKAGE);
		gap_plan_free(&plan);
		return 1;
	}

	if ((size_t)size < existing_blocks) {
		existing_blocks = (size_t)size;
	}

	if ((size_t)size > existing_blocks) {
		size_t missing = (size_t)size - existing_blocks;
		if (gap_plan_add(&plan, existing_blocks, missing) != 0) {
			gap_plan_free(&plan);
			return 1;
		}
		truncated_blocks = missing;
	}

	sample_count = gap_collect_samples(&plan, (size_t)size, GAP_SAMPLE_TARGET, sample_slots);
	if (sample_count > 0) {
		if (gap_verify_samples(destination, dvd_file, offset,
				label ? label : path, sample_slots, sample_count) != 0) {
			gap_plan_free(&plan);
			return 1;
		}
	}

	size_t blank_after = blank_blocks;
	size_t truncated_after = truncated_blocks;
	size_t filled_blocks = 0;
	int fill_status = gap_fill_from_plan(destination, dvd_file, offset, &plan,
			label ? label : path, errorstrat, &filled_blocks);

	gap_plan_free(&plan);

	if (fill_status == 0) {
		gap_plan_t verify_plan = (gap_plan_t){0};
		size_t verify_blank = 0;
		size_t verify_existing_blocks = 0;
		off_t verify_bytes = 0;

		if (scan_existing_file_for_gaps(destination, (size_t)size, &verify_plan,
				&verify_blank, &verify_existing_blocks, &verify_bytes) == 0) {
			blank_after = verify_blank;
			if ((size_t)size > verify_existing_blocks) {
				truncated_after = (size_t)size - verify_existing_blocks;
			} else {
				truncated_after = 0;
			}
		}
		gap_plan_free(&verify_plan);
	}

	gap_print_report(path, (size_t)size,
			blank_blocks, truncated_blocks,
			blank_after, truncated_after,
			filled_blocks);

	if (fill_status != 0) {
		result = 1;
	}

	return result;
}


static int DVDCopyBlocks(dvd_file_t* dvd_file, int destination, int offset, int size,
		const char* path, const char* label, read_error_strategy_t errorstrat) {
	int i;

	if (fill_gaps) {
		return DVDCopyBlocksFillGaps(dvd_file, destination, offset, size, path, label, errorstrat);
	}

	/* all sizes are in DVD logical blocks */
	int remaining = size;
	int total = size; // total size in blocks
	float totalMiB = (float)(total) / 512.0f; // total size in [MiB]
	int to_read = BUFFER_SIZE;
	int act_read; /* number of buffers actually read */

	/* Write buffer */
	unsigned char buffer[BUFFER_SIZE * DVD_VIDEO_LB_LEN];
	unsigned char buffer_zero[BUFFER_SIZE * DVD_VIDEO_LB_LEN];

	for(i = 0; i < BUFFER_SIZE * DVD_VIDEO_LB_LEN; i++) {
		buffer_zero[i] = '\0';
	}

	while( remaining > 0 ) {

		if (to_read > remaining) {
			to_read = remaining;
		}

		/* Reading blocks */
		act_read = DVDReadBlocks(dvd_file, offset, to_read, buffer);

		if(act_read != to_read) {
			if(progress) {
				fprintf(stdout, "\n");
			}
			if(act_read >= 0) {
				fprintf(stderr, _("Error reading %s at block %d\n"), label, offset+act_read);
			} else {
				fprintf(stderr, _("Error reading %s at block %d, read error returned\n"), label, offset);
			}
		}

		if(act_read > 0) {
			/* Writing blocks */
			if(write(destination, buffer, act_read * DVD_VIDEO_LB_LEN) != act_read * DVD_VIDEO_LB_LEN) {
				if(progress) {
					fprintf(stdout, "\n");
				}
				fprintf(stderr, _("Error writing %s.\n"), label);
				return(1);
			}

			offset += act_read;
			remaining -= act_read;
		}

		if(act_read != to_read) {
			int numBlanks = 0;

			if(progress) {
				fprintf(stdout, "\n");
			}

			if (act_read < 0) {
				act_read = 0;
			}

			switch (errorstrat) {
			case STRATEGY_ABORT:
				fprintf(stderr, _("aborting\n"));
				return 1;

			case STRATEGY_SKIP_BLOCK:
				numBlanks = 1;
				fprintf(stderr, _("padding single block\n"));
				break;

			case STRATEGY_SKIP_MULTIBLOCK:
				numBlanks = to_read - act_read;
				fprintf(stderr, _("padding %d blocks\n"), numBlanks);
				break;
			}

			if (write(destination, buffer_zero, numBlanks * DVD_VIDEO_LB_LEN) != numBlanks * DVD_VIDEO_LB_LEN) {
				fprintf(stderr, _("Error writing %s (padding)\n"), label);
				return 1;
			}

			/* pretend we read what we padded */
			offset += numBlanks;
			remaining -= numBlanks;
		}

		if(progress) {
			int done = total - remaining; // blocks done
			if(remaining < BUFFER_SIZE || (done % BUFFER_SIZE) == 0) { // don't print too often
				float doneMiB = (float)(done) / 512.0f; // [MiB] done
				fprintf(stdout, "\r");
				fprintf(stdout, _("Copying %s: %.0f%% done (%.0f/%.0f MiB)"),
						progressText, doneMiB / totalMiB * 100.0f, doneMiB, totalMiB);
				fflush(stdout);
			}
		}

	}

	if(progress) {
		fprintf(stdout, "\n");
	}

	return 0;
}



static int DVDCopyTitleVobX(dvd_reader_t * dvd, title_set_info_t * title_set_info, int title_set, int vob, char * targetdir,char * title_name, read_error_strategy_t errorstrat) {

	/* Loop variable */
	int i;

	/* Temp filename,dirname */
	// filename is either "VIDEO_TS.VOB" or "VTS_XX_X.VOB" and terminating "\0"
	char filename[13] = "VIDEO_TS.VOB";
	char *targetname;
	size_t targetname_length;
	struct stat fileinfo;

	/* File Handler */
	int streamout;

	int size;

	int offset = 0;
	int tsize;

	/* DVD handler */
	dvd_file_t* dvd_file=NULL;

	/* Return value */
	int result;


	/* create filename VIDEO_TS.VOB or VTS_XX_X.VOB */
	if(title_set > 0) {
		sprintf(filename, "VTS_%02i_%1i.VOB", title_set, vob);
	}

	if (title_set_info->number_of_title_sets + 1 < title_set) {
		fprintf(stderr,_("Failed num title test\n"));
		return(1);
	}

	if (title_set_info->title_set[title_set].number_of_vob_files < vob ) {
		fprintf(stderr,_("Failed vob test\n"));
		return(1);
	}

	if (title_set_info->title_set[title_set].size_vob[0] == 0 ) {
		fprintf(stderr,_("Failed vob 1 size test\n"));
		return(0);
	} else if (title_set_info->title_set[title_set].size_vob[vob - 1] == 0 ) {
		fprintf(stderr,_("Failed vob %d test\n"), vob);
		return(0);
	} else {
		size = title_set_info->title_set[title_set].size_vob[vob - 1]/DVD_VIDEO_LB_LEN;
		if (title_set_info->title_set[title_set].size_vob[vob - 1]%DVD_VIDEO_LB_LEN != 0) {
			fprintf(stderr, _("The Title VOB number %d of title set %d does not have a valid DVD size\n"), vob, title_set);
			return(1);
		}
	}
#ifdef DEBUG
	fprintf(stderr,"After we check the vob it self %d\n", vob);
#endif

	/* Create VTS_XX_X.VOB */
	if (title_set == 0) {
		fprintf(stderr,_("Do not try to copy a Title VOB from the VMG domain; there are none.\n"));
		return(1);
	} else {
		// Reserve space for "<targetdir>/<title_name>/VIDEO_TS/<filename>" and terminating "\0"
		targetname_length = strlen(targetdir) + strlen(title_name) + strlen(filename) + 12;
		targetname = malloc(targetname_length);
		if (targetname == NULL) {
			fprintf(stderr, _("Failed to allocate %zu bytes for a filename.\n"), targetname_length);
			return 1;
		}
		snprintf(targetname, targetname_length, "%s/%s/VIDEO_TS/%s", targetdir, title_name, filename);
	}



	/* Now figure out the offset we will start at also check that the previus files are of valid DVD size */
	for ( i = 0; i < vob - 1; i++ ) {
		tsize = title_set_info->title_set[title_set].size_vob[i];
		if (tsize%DVD_VIDEO_LB_LEN != 0) {
			fprintf(stderr, _("The Title VOB number %d of title set %d does not have a valid DVD size\n"), i + 1, title_set);
			free(targetname);
			return(1);
		} else {
			offset = offset + tsize/DVD_VIDEO_LB_LEN;
		}
	}
#ifdef DEBUG
	fprintf(stderr,"The offset for vob %d is %d\n", vob, offset);
#endif


	if (stat(targetname, &fileinfo) == 0) {
		if (! S_ISREG(fileinfo.st_mode)) {
			/* TRANSLATORS: The sentence starts with "The title file %s is not valid[...]" */
			fprintf(stderr,_("The %s %s is not valid, it may be a directory.\n"), _("title file"), targetname);
			free(targetname);
			return(1);
		}
		if (fill_gaps) {
			fprintf(stderr, _("The %s %s exists; checking for gaps.\n"), _("title file"), targetname);
			streamout = open(targetname, O_RDWR, 0666);
		} else {
			fprintf(stderr, _("The %s %s exists; truncating before copy.\n"), _("title file"), targetname);
			streamout = open(targetname, O_WRONLY | O_TRUNC, 0666);
		}
		if (streamout == -1) {
			fprintf(stderr, _("Error opening %s\n"), targetname);
			perror(PACKAGE);
			free(targetname);
			return(1);
		}
	} else {
		int create_flags = fill_gaps ? (O_RDWR | O_CREAT) : (O_WRONLY | O_CREAT);
		if ((streamout = open(targetname, create_flags, 0666)) == -1) {
			fprintf(stderr, _("Error creating %s\n"), targetname);
			perror(PACKAGE);
			free(targetname);
			return(1);
		}
	}

	if ((dvd_file = DVDOpenFile(dvd, title_set, DVD_READ_TITLE_VOBS))== 0) {
		fprintf(stderr, _("Failed opening TITLE VOB\n"));
		close(streamout);
		free(targetname);
		return(1);
	}

	result = DVDCopyBlocks(dvd_file, streamout, offset, size, targetname, filename, errorstrat);

	DVDCloseFile(dvd_file);
	close(streamout);
	free(targetname);
	return result;
}


static int DVDCmpTitleVobX(dvd_reader_t * dvd, title_set_info_t * title_set_info, int title_set, int vob, char * targetdir,char * title_name, read_error_strategy_t errorstrat) {
	char filename[13] = "VIDEO_TS.VOB";
	char *targetname;
	size_t targetname_length;
	struct stat fileinfo;
	dvd_file_t* dvd_file = NULL;
	int fd = -1;
	int size;
	int offset = 0;
	int tsize;

	(void)errorstrat;

	if(title_set > 0) {
		sprintf(filename, "VTS_%02i_%1i.VOB", title_set, vob);
	}

	if (title_set_info->number_of_title_sets + 1 < title_set) {
		return 1;
	}

	if (title_set_info->title_set[title_set].number_of_vob_files < vob ) {
		return 1;
	}

	if (title_set_info->title_set[title_set].size_vob[0] == 0 ) {
		return 1;
	} else if (title_set_info->title_set[title_set].size_vob[vob - 1] == 0 ) {
		return 1;
	} else {
		size = title_set_info->title_set[title_set].size_vob[vob - 1]/DVD_VIDEO_LB_LEN;
		if (title_set_info->title_set[title_set].size_vob[vob - 1]%DVD_VIDEO_LB_LEN != 0) {
			return 1;
		}
	}

	for (int i = 0; i < vob - 1; i++) {
		tsize = title_set_info->title_set[title_set].size_vob[i];
		if (tsize % DVD_VIDEO_LB_LEN != 0) {
			return 1;
		}
		offset += tsize / DVD_VIDEO_LB_LEN;
	}

	if ((dvd_file = DVDOpenFile(dvd, title_set, DVD_READ_TITLE_VOBS)) == 0) {
		return 1;
	}

	targetname_length = strlen(targetdir) + strlen(title_name) + strlen(filename) + 12;
	targetname = malloc(targetname_length);
	if (targetname == NULL) {
		DVDCloseFile(dvd_file);
		return 1;
	}
	snprintf(targetname, targetname_length, "%s/%s/VIDEO_TS/%s", targetdir, title_name, filename);

	if (stat(targetname, &fileinfo) != 0 || !S_ISREG(fileinfo.st_mode)) {
		if (gap_map) {
			size_t base = gap_map_total_blocks;
			gap_map_collect_missing(base, (size_t)size);
			gap_map_total_blocks += size;
		}
		free(targetname);
		DVDCloseFile(dvd_file);
		return 1;
	}

	off_t expected_bytes = (off_t)size * DVD_VIDEO_LB_LEN;
	if (fileinfo.st_size != expected_bytes) {
		if (gap_map) {
			size_t base = gap_map_total_blocks;
			gap_map_collect_missing(base, (size_t)size);
			gap_map_total_blocks += size;
		}
		free(targetname);
		DVDCloseFile(dvd_file);
		return 1;
	}

	fd = open(targetname, O_RDONLY);
	if (fd == -1) {
		perror(PACKAGE);
		free(targetname);
		DVDCloseFile(dvd_file);
		return 1;
	}

	if (progress) {
		snprintf(progressText, MAXNAME, _("Title, part %i"), vob);
	}

	if (gap_map) {
		size_t base = gap_map_total_blocks;
		gap_plan_t plan = {0};
		size_t blank_blocks = 0;
		size_t full_blocks = 0;
		off_t existing_bytes = 0;
		if (scan_existing_file_for_gaps(fd, (size_t)size, &plan, &blank_blocks, &full_blocks, &existing_bytes) == 0) {
			gap_map_collect_from_plan(base, (size_t)size, &plan, full_blocks);
		} else {
			gap_map_collect_missing(base, (size_t)size);
		}
		gap_plan_free(&plan);
		gap_map_total_blocks += size;
		if (lseek(fd, (off_t)offset * DVD_VIDEO_LB_LEN, SEEK_SET) == (off_t)-1) {
			perror(PACKAGE);
			close(fd);
			free(targetname);
			DVDCloseFile(dvd_file);
			return 1;
		}
	}

	int cmp = DVDCmpBlocks(dvd_file, fd, offset, size, targetname, filename, errorstrat);

	close(fd);
	free(targetname);
	DVDCloseFile(dvd_file);
	return cmp;
}


static int DVDCopyMenu(dvd_reader_t * dvd, title_set_info_t * title_set_info, int title_set, char * targetdir,char * title_name, read_error_strategy_t errorstrat) {

	/* Temp filename,dirname */
	// filename is either "VIDEO_TS.VOB" or "VTS_XX_0.VOB" and terminating "\0"
	char filename[13] = "VIDEO_TS.VOB";
	char *targetname;
	size_t targetname_length;
	struct stat fileinfo;

	/* File Handler */
	int streamout;

	int size;

	/* return value */
	int result;

	/* DVD handler */
	dvd_file_t* dvd_file = NULL;

	/* create filename VIDEO_TS.VOB or VTS_XX_0.VOB */
	if(title_set > 0) {
		sprintf(filename, "VTS_%02i_0.VOB", title_set);
	}

	if (title_set_info->number_of_title_sets + 1 < title_set) {
		return(1);
	}

	if (title_set_info->title_set[title_set].size_menu == 0 ) {
		return(0);
	} else {
		size = title_set_info->title_set[title_set].size_menu/DVD_VIDEO_LB_LEN;
		if (title_set_info->title_set[title_set].size_menu%DVD_VIDEO_LB_LEN != 0) {
			fprintf(stderr, _("Warning: The Menu VOB of title set %d (%s) does not have a valid DVD size.\n"), title_set, filename);
		}
	}

	if ((dvd_file = DVDOpenFile(dvd, title_set, DVD_READ_MENU_VOBS))== 0) {
		fprintf(stderr, _("Failed opening %s\n"), filename);
		return(1);
	}

	// Reserve space for "<targetdir>/<title_name>/VIDEO_TS/<filename>" and terminating "\0"
	targetname_length = strlen(targetdir) + strlen(title_name) + strlen(filename) + 12;
	targetname = malloc(targetname_length);
	if (targetname == NULL) {
		fprintf(stderr, _("Failed to allocate %zu bytes for a filename.\n"), targetname_length);
		return 1;
	}
	/* Create VIDEO_TS.VOB or VTS_XX_0.VOB */
	snprintf(targetname, targetname_length, "%s/%s/VIDEO_TS/%s", targetdir, title_name, filename);

	if (stat(targetname, &fileinfo) == 0) {
		if (! S_ISREG(fileinfo.st_mode)) {
			/* TRANSLATORS: The sentence starts with "The menu file %s is not valid[...]" */
			fprintf(stderr,_("The %s %s is not valid, it may be a directory.\n"), _("menu file"), targetname);
			DVDCloseFile(dvd_file);
			free(targetname);
			return(1);
		}
		if (fill_gaps) {
			fprintf(stderr, _("The %s %s exists; checking for gaps.\n"), _("menu file"), targetname);
			streamout = open(targetname, O_RDWR, 0666);
		} else {
			/* TRANSLATORS: The sentence starts with "The menu file %s exists[...]" */
			fprintf(stderr, _("The %s %s exists; truncating before copy.\n"), _("menu file"), targetname);
			streamout = open(targetname, O_WRONLY | O_TRUNC, 0666);
		}
		if (streamout == -1) {
			fprintf(stderr, _("Error opening %s\n"), targetname);
			perror(PACKAGE);
			DVDCloseFile(dvd_file);
			free(targetname);
			return(1);
		}
	} else {
		int create_flags = fill_gaps ? (O_RDWR | O_CREAT) : (O_WRONLY | O_CREAT);
		if ((streamout = open(targetname, create_flags, 0666)) == -1) {
			fprintf(stderr, _("Error creating %s\n"), targetname);
			perror(PACKAGE);
			DVDCloseFile(dvd_file);
			free(targetname);
			return(1);
		}
	}

	if(progress) {
		strncpy(progressText, _("menu"), MAXNAME);
	}

	result = DVDCopyBlocks(dvd_file, streamout, 0, size, targetname, filename, errorstrat);

	DVDCloseFile(dvd_file);
	close(streamout);
	free(targetname);
	return result;

}


static int DVDCmpMenu(dvd_reader_t * dvd, title_set_info_t * title_set_info, int title_set, char * targetdir,char * title_name, read_error_strategy_t errorstrat) {
	char filename[13] = "VIDEO_TS.VOB";
	char *targetname;
	size_t targetname_length;
	struct stat fileinfo;
	dvd_file_t* dvd_file = NULL;
	int fd = -1;
	int size;

	(void)errorstrat;

	if(title_set > 0) {
		sprintf(filename, "VTS_%02i_0.VOB", title_set);
	}

	if (title_set_info->number_of_title_sets + 1 < title_set) {
		return 1;
	}

	if (title_set_info->title_set[title_set].size_menu == 0) {
		return 0;
	}

	if (title_set_info->title_set[title_set].size_menu % DVD_VIDEO_LB_LEN != 0) {
		fprintf(stderr, _("Warning: The Menu VOB of title set %d (%s) does not have a valid DVD size.\n"), title_set, filename);
		return 1;
	}

	if ((dvd_file = DVDOpenFile(dvd, title_set, DVD_READ_MENU_VOBS)) == 0) {
		fprintf(stderr, _("Failed opening %s\n"), filename);
		return 1;
	}

	size = title_set_info->title_set[title_set].size_menu / DVD_VIDEO_LB_LEN;
	targetname_length = strlen(targetdir) + strlen(title_name) + strlen(filename) + 12;
	targetname = malloc(targetname_length);
	if (targetname == NULL) {
		fprintf(stderr, _("Failed to allocate %zu bytes for a filename.\n"), targetname_length);
		DVDCloseFile(dvd_file);
		return 1;
	}
	snprintf(targetname, targetname_length, "%s/%s/VIDEO_TS/%s", targetdir, title_name, filename);

	if (stat(targetname, &fileinfo) != 0 || !S_ISREG(fileinfo.st_mode)) {
		fprintf(stderr, _("Cannot compare %s; file is missing or invalid.\n"), targetname);
		if (gap_map) {
			size_t base = gap_map_total_blocks;
			gap_map_collect_missing(base, (size_t)size);
			gap_map_total_blocks += size;
		}
		free(targetname);
		DVDCloseFile(dvd_file);
		return 1;
	}

	off_t expected_bytes = (off_t)size * DVD_VIDEO_LB_LEN;
	if (fileinfo.st_size != expected_bytes) {
		fprintf(stderr, _("Size mismatch for %s: expected %lld bytes, found %lld bytes.\n"),
			targetname, (long long)expected_bytes, (long long)fileinfo.st_size);
		if (gap_map) {
			size_t base = gap_map_total_blocks;
			gap_map_collect_missing(base, (size_t)size);
			gap_map_total_blocks += size;
		}
		free(targetname);
		DVDCloseFile(dvd_file);
		return 1;
	}

	fd = open(targetname, O_RDONLY);
	if (fd == -1) {
		fprintf(stderr, _("Error opening %s\n"), targetname);
		perror(PACKAGE);
		free(targetname);
		DVDCloseFile(dvd_file);
		return 1;
	}

	if (progress) {
		strncpy(progressText, _("menu"), MAXNAME);
	}

	if (gap_map) {
		size_t base = gap_map_total_blocks;
		gap_plan_t plan = {0};
		size_t blank_blocks = 0;
		size_t full_blocks = 0;
		off_t existing_bytes = 0;
		if (scan_existing_file_for_gaps(fd, (size_t)size, &plan, &blank_blocks, &full_blocks, &existing_bytes) == 0) {
			gap_map_collect_from_plan(base, (size_t)size, &plan, full_blocks);
		} else {
			gap_map_collect_missing(base, (size_t)size);
		}
		gap_plan_free(&plan);
		gap_map_total_blocks += size;
		if (lseek(fd, 0, SEEK_SET) == (off_t)-1) {
			perror(PACKAGE);
			close(fd);
			free(targetname);
			DVDCloseFile(dvd_file);
			return 1;
		}
	}

	int cmp = DVDCmpBlocks(dvd_file, fd, 0, size, targetname, filename, errorstrat);

	close(fd);
	free(targetname);
	DVDCloseFile(dvd_file);
	return cmp;
}


static int DVDCopyIfoBup(dvd_reader_t* dvd, title_set_info_t* title_set_info, int title_set, char* targetdir, char* title_name) {
	/* Temp filename, dirname */
	char *targetname_ifo;
	char *targetname_bup;
	size_t string_length;
	struct stat fileinfo;

	/* Write buffer */
	unsigned char* buffer = NULL;

	/* File Handler */
	int streamout_ifo = -1, streamout_bup = -1;

	int size;

	/* DVD handler */
	ifo_handle_t* ifo_file = NULL;


	if (title_set_info->number_of_title_sets + 1 < title_set) {
		return(1);
	}

	if (title_set_info->title_set[title_set].size_ifo == 0 ) {
		return(0);
	} else {
		if (title_set_info->title_set[title_set].size_ifo%DVD_VIDEO_LB_LEN != 0) {
			fprintf(stderr, _("The IFO of title set %d does not have a valid DVD size\n"), title_set);
			return(1);
		}
	}

	// Reserve space for "<targetdir>/<title_name>/VIDEO_TS/VIDEO_TS.IFO" or
	// "<targetdir>/<title_name>/VIDEO_TS/VTS_XX_0.IFO" and terminating "\0"
	string_length = strlen(targetdir) + strlen(title_name) + 24;
	targetname_ifo = malloc(string_length);
	targetname_bup = malloc(string_length);
	if (targetname_ifo == NULL || targetname_bup == NULL) {
		fprintf(stderr, _("Failed to allocate %zu bytes for a filename.\n"), string_length);
		free(targetname_ifo);
		free(targetname_bup);
		return 1;
	}

	/* Create VIDEO_TS.IFO or VTS_XX_0.IFO */

	if (title_set == 0) {
		snprintf(targetname_ifo, string_length, "%s/%s/VIDEO_TS/VIDEO_TS.IFO", targetdir, title_name);
		snprintf(targetname_bup, string_length, "%s/%s/VIDEO_TS/VIDEO_TS.BUP", targetdir, title_name);
	} else {
		snprintf(targetname_ifo, string_length, "%s/%s/VIDEO_TS/VTS_%02i_0.IFO", targetdir, title_name, title_set);
		snprintf(targetname_bup, string_length, "%s/%s/VIDEO_TS/VTS_%02i_0.BUP", targetdir, title_name, title_set);
	}

	if (stat(targetname_ifo, &fileinfo) == 0) {
		/* TRANSLATORS: The sentence starts with "The IFO file %s exists[...]" */
		if (fill_gaps) {
			fprintf(stderr, _("The %s %s exists; refreshing it for --gaps.\n"), _("IFO file"), targetname_ifo);
		} else {
			fprintf(stderr, _("The %s %s exists; truncating before copy.\n"), _("IFO file"), targetname_ifo);
		}
		if (! S_ISREG(fileinfo.st_mode)) {
			/* TRANSLATORS: The sentence starts with "The IFO file %s is not valid[...]" */
			fprintf(stderr,_("The %s %s is not valid, it may be a directory.\n"), _("IFO file"), targetname_ifo);
			free(targetname_ifo);
			free(targetname_bup);
			return(1);
		}
	}

	if (stat(targetname_bup, &fileinfo) == 0) {
		/* TRANSLATORS: The sentence starts with "The BUP file %s exists[...]" */
		if (fill_gaps) {
			fprintf(stderr, _("The %s %s exists; refreshing it for --gaps.\n"), _("BUP file"), targetname_bup);
		} else {
			fprintf(stderr, _("The %s %s exists; truncating before copy.\n"), _("BUP file"), targetname_bup);
		}
		if (! S_ISREG(fileinfo.st_mode)) {
			/* TRANSLATORS: The sentence starts with "The BUP file %s is not valid[...]" */
			fprintf(stderr,_("The %s %s is not valid, it may be a directory.\n"), _("BUP file"), targetname_bup);
			free(targetname_ifo);
			free(targetname_bup);
			return(1);
		}
	}

	if ((streamout_ifo = open(targetname_ifo, O_WRONLY | O_CREAT | O_TRUNC, 0666)) == -1) {
		fprintf(stderr, _("Error creating %s\n"), targetname_ifo);
		perror(PACKAGE);
		ifoClose(ifo_file);
		free(buffer);
		free(targetname_ifo);
		free(targetname_bup);
		close(streamout_ifo);
		close(streamout_bup);
		return 1;
	}

	if ((streamout_bup = open(targetname_bup, O_WRONLY | O_CREAT | O_TRUNC, 0666)) == -1) {
		fprintf(stderr, _("Error creating %s\n"), targetname_bup);
		perror(PACKAGE);
		ifoClose(ifo_file);
		free(buffer);
		free(targetname_ifo);
		free(targetname_bup);
		close(streamout_ifo);
		close(streamout_bup);
		return 1;
	}

	/* Copy VIDEO_TS.IFO, since it's a small file try to copy it in one shot */

	if ((ifo_file = ifoOpen(dvd, title_set))== 0) {
		fprintf(stderr, _("Failed opening IFO for title set %d\n"), title_set);
		ifoClose(ifo_file);
		free(buffer);
		free(targetname_ifo);
		free(targetname_bup);
		close(streamout_ifo);
		close(streamout_bup);
		return 1;
	}

	size = DVDFileSize(ifo_file->file) * DVD_VIDEO_LB_LEN;

	if ((buffer = (unsigned char *)malloc(size * sizeof(unsigned char))) == NULL) {
		perror(PACKAGE);
		ifoClose(ifo_file);
		free(buffer);
		free(targetname_ifo);
		free(targetname_bup);
		close(streamout_ifo);
		close(streamout_bup);
		return 1;
	}

	DVDFileSeek(ifo_file->file, 0);

	if (DVDReadBytes(ifo_file->file,buffer,size) != size) {
		fprintf(stderr, _("Error reading IFO for title set %d\n"), title_set);
		ifoClose(ifo_file);
		free(buffer);
		free(targetname_ifo);
		free(targetname_bup);
		close(streamout_ifo);
		close(streamout_bup);
		return 1;
	}


	if (write(streamout_ifo,buffer,size) != size) {
		fprintf(stderr, _("Error writing %s\n"),targetname_ifo);
		ifoClose(ifo_file);
		free(buffer);
		free(targetname_ifo);
		free(targetname_bup);
		close(streamout_ifo);
		close(streamout_bup);
		return 1;
	}

	if (write(streamout_bup,buffer,size) != size) {
		fprintf(stderr, _("Error writing %s\n"),targetname_bup);
		ifoClose(ifo_file);
		free(buffer);
		free(targetname_ifo);
		free(targetname_bup);
		close(streamout_ifo);
		close(streamout_bup);
		return 1;
	}

	ifoClose(ifo_file);
	free(buffer);
	close(streamout_ifo);
	close(streamout_bup);

	free(targetname_ifo);
	free(targetname_bup);
	return 0;
}

static int DVDCmpIfoBup(dvd_reader_t* dvd, title_set_info_t* title_set_info, int title_set, char* targetdir, char* title_name, read_error_strategy_t errorstrat) {
	char *targetname_ifo = NULL;
	char *targetname_bup = NULL;
	size_t string_length;
	struct stat fileinfo;
	dvd_file_t* dvd_file = NULL;
	int fd = -1;
	int blocks;
	char ifo_label[16];

	if (title_set_info->number_of_title_sets + 1 < title_set) {
		return 1;
	}

	if (title_set_info->title_set[title_set].size_ifo == 0) {
		return 0;
	}

	if (title_set_info->title_set[title_set].size_ifo % DVD_VIDEO_LB_LEN != 0) {
		fprintf(stderr, _("The IFO of title set %d does not have a valid DVD size\n"), title_set);
		return 1;
	}

	blocks = title_set_info->title_set[title_set].size_ifo / DVD_VIDEO_LB_LEN;

	string_length = strlen(targetdir) + strlen(title_name) + 24;
	targetname_ifo = malloc(string_length);
	targetname_bup = malloc(string_length);
	if (targetname_ifo == NULL || targetname_bup == NULL) {
		fprintf(stderr, _("Failed to allocate %zu bytes for a filename.\n"), string_length);
		free(targetname_ifo);
		free(targetname_bup);
		return 1;
	}

	if (title_set == 0) {
		snprintf(targetname_ifo, string_length, "%s/%s/VIDEO_TS/VIDEO_TS.IFO", targetdir, title_name);
		snprintf(targetname_bup, string_length, "%s/%s/VIDEO_TS/VIDEO_TS.BUP", targetdir, title_name);
		snprintf(ifo_label, sizeof(ifo_label), "VIDEO_TS.IFO");
	} else {
		snprintf(targetname_ifo, string_length, "%s/%s/VIDEO_TS/VTS_%02i_0.IFO", targetdir, title_name, title_set);
		snprintf(targetname_bup, string_length, "%s/%s/VIDEO_TS/VTS_%02i_0.BUP", targetdir, title_name, title_set);
		snprintf(ifo_label, sizeof(ifo_label), "VTS_%02d_0.IFO", title_set);
	}

	if (stat(targetname_ifo, &fileinfo) != 0 || !S_ISREG(fileinfo.st_mode)) {
		fprintf(stderr, _("Cannot compare %s; file is missing or invalid.\n"), targetname_ifo);
		goto cmp_ifo_cleanup;
	}

	if (stat(targetname_bup, &fileinfo) != 0 || !S_ISREG(fileinfo.st_mode)) {
		fprintf(stderr, _("Cannot compare %s; file is missing or invalid.\n"), targetname_bup);
		goto cmp_ifo_cleanup;
	}

	dvd_file = DVDOpenFile(dvd, title_set, DVD_READ_INFO_FILE);
	if (dvd_file == NULL) {
		fprintf(stderr, _("Failed opening info file for title set %d\n"), title_set);
		goto cmp_ifo_cleanup;
	}

	fd = open(targetname_ifo, O_RDONLY);
	if (fd == -1) {
		fprintf(stderr, _("Error opening %s\n"), targetname_ifo);
		perror(PACKAGE);
		goto cmp_ifo_cleanup;
	}

	if (gap_map) {
		size_t base = gap_map_total_blocks;
		gap_plan_t plan = {0};
		size_t blank_blocks = 0;
		size_t full_blocks = 0;
		off_t existing_bytes = 0;
		if (scan_existing_file_for_gaps(fd, (size_t)blocks, &plan, &blank_blocks, &full_blocks, &existing_bytes) == 0) {
			gap_map_collect_from_plan(base, (size_t)blocks, &plan, full_blocks);
		} else {
			gap_map_collect_missing(base, (size_t)blocks);
		}
		gap_plan_free(&plan);
		gap_map_total_blocks += blocks;
		if (lseek(fd, 0, SEEK_SET) == (off_t)-1) {
			perror(PACKAGE);
			goto cmp_ifo_cleanup;
		}
	}

	if (DVDCmpBlocks(dvd_file, fd, 0, blocks, targetname_ifo, ifo_label, errorstrat) != 0) {
		goto cmp_ifo_cleanup;
	}

	close(fd);
	DVDCloseFile(dvd_file);
	dvd_file = NULL;

	dvd_file = DVDOpenFile(dvd, title_set, DVD_READ_INFO_FILE);
	if (dvd_file == NULL) {
		fprintf(stderr, _("Failed reopening info file for title set %d\n"), title_set);
		goto cmp_ifo_cleanup;
	}

	fd = open(targetname_bup, O_RDONLY);
	if (fd == -1) {
		fprintf(stderr, _("Error opening %s\n"), targetname_bup);
		perror(PACKAGE);
		goto cmp_ifo_cleanup;
	}

	if (gap_map) {
		size_t base = gap_map_total_blocks;
		gap_plan_t plan = {0};
		size_t blank_blocks = 0;
		size_t full_blocks = 0;
		off_t existing_bytes = 0;
		if (scan_existing_file_for_gaps(fd, (size_t)blocks, &plan, &blank_blocks, &full_blocks, &existing_bytes) == 0) {
			gap_map_collect_from_plan(base, (size_t)blocks, &plan, full_blocks);
		} else {
			gap_map_collect_missing(base, (size_t)blocks);
		}
		gap_plan_free(&plan);
		gap_map_total_blocks += blocks;
		if (lseek(fd, 0, SEEK_SET) == (off_t)-1) {
			perror(PACKAGE);
			goto cmp_ifo_cleanup;
		}
	}

	if (DVDCmpBlocks(dvd_file, fd, 0, blocks, targetname_bup, ifo_label, errorstrat) != 0) {
		goto cmp_ifo_cleanup;
	}

	close(fd);
	DVDCloseFile(dvd_file);
	free(targetname_ifo);
	free(targetname_bup);
	return 0;

cmp_ifo_cleanup:
	if (fd != -1) {
		close(fd);
	}
	if (dvd_file) {
		DVDCloseFile(dvd_file);
	}
	if (targetname_ifo) {
		free(targetname_ifo);
	}
	if (targetname_bup) {
		free(targetname_bup);
	}
	return 1;
}


static int DVDMirrorTitleX(dvd_reader_t* dvd, title_set_info_t* title_set_info,
		int title_set, char* targetdir, char* title_name,
		read_error_strategy_t errorstrat) {

	/* Loop through the vobs */
	int i;
	int n;

	if (compare_only) {
		if ( DVDCmpIfoBup(dvd, title_set_info, title_set, targetdir, title_name, errorstrat) != 0 ) {
			return 1;
		}
	} else if ( DVDCopyIfoBup(dvd, title_set_info, title_set, targetdir, title_name) != 0 ) {
		return 1;
	}

	if (compare_only) {
		if ( DVDCmpMenu(dvd, title_set_info, title_set, targetdir, title_name, errorstrat) != 0 ) {
			return 1;
		}
	} else if ( DVDCopyMenu(dvd, title_set_info, title_set, targetdir, title_name, errorstrat) != 0 ) {
		return 1;
	}

	n = title_set_info->title_set[title_set].number_of_vob_files;
	for (i = 0; i < n; i++) {
#ifdef DEBUG
		fprintf(stderr,"In the VOB copy loop for %d\n", i);
#endif
		if(progress) {
			snprintf(progressText, MAXNAME, _("Title, part %i/%i"), i+1, n);
		}
		if (compare_only) {
			if ( DVDCmpTitleVobX(dvd, title_set_info, title_set, i + 1, targetdir, title_name, errorstrat) != 0 ) {
				return 1;
			}
		} else if ( DVDCopyTitleVobX(dvd, title_set_info, title_set, i + 1, targetdir, title_name, errorstrat) != 0 ) {
			return 1;
		}
	}


	return(0);
}

int DVDGetTitleName(const char *device, char *title)
{
	/* Variables for filehandel and title string interaction */

	char tempBuf[DVD_SEC_SIZ];
	int filehandle, i;
	int length = 32;
	int word_length = 0;

	/* Open DVD device */

	if ( !(filehandle = open(device, O_RDONLY)) ) {
		fprintf(stderr, _("Cannot open specified device %s - check your DVD device\n"), device);
		return(1);
	}

	/* Seek to title of first track, which is at (track_no * 32768) + 40 */

	if(lseek(filehandle, 32768, SEEK_SET) != 32768) {
		close(filehandle);
		fprintf(stderr, _("Cannot seek DVD device %s - check your DVD device\n"), device);
		return(1);
	}

	/* Read the DVD-Video title */
	if(DVD_SEC_SIZ != read(filehandle, tempBuf, DVD_SEC_SIZ)) {
		close(filehandle);
		fprintf(stderr, _("Cannot read title from DVD device %s\n"), device);
		return(1);
	}
	snprintf(title, length + 1, "%s", tempBuf + 40);

	/* Remove trailing white space */
	while(title[length-1] == ' ') {
		title[length-1] = '\0';
		length--;
	}

	/* convert title to lower case and replace underscores with spaces */
	for(i = 0; i < length; i++) {
		word_length++;
		if(word_length == 1) {
			title[i] = toupper(title[i]);
		} else {
			title[i] = tolower(title[i]);
		}
		if(title[i] == '_') {
			title[i] = ' ';
		}
		if(title[i] == ' ') {
			word_length = 0;
		}
	}

	return(0);
}



static void bsort_min_to_max(int sector[], int title[], int size){

	int temp_title, temp_sector, i, j;

	for ( i=0; i < size ; i++ ) {
		for ( j=0; j < size ; j++ ) {
			if (sector[i] < sector[j]) {
				temp_sector = sector[i];
				temp_title = title[i];
				sector[i] = sector[j];
				title[i] = title[j];
				sector[j] = temp_sector;
				title[j] = temp_title;
			}
		}
	}
}

static void bsort_max_to_min(int sector[], int title[], int size){

	int temp_title, temp_sector, i, j;

	for ( i=0; i < size ; i++ ) {
		for ( j=0; j < size ; j++ ) {
			if (sector[i] > sector[j]) {
				temp_sector = sector[i];
				temp_title = title[i];
				sector[i] = sector[j];
				title[i] = title[j];
				sector[j] = temp_sector;
				title[j] = temp_title;
			}
		}
	}
}


static void align_end_sector(int cell_start_sector[],int cell_end_sector[], int size) {

	int i;

	for (i = 0; i < size - 1 ; i++) {
		if ( cell_end_sector[i] >= cell_start_sector[i + 1] ) {
			cell_end_sector[i] = cell_start_sector[i + 1] - 1;
		}
	}
}


static void DVDFreeTitleSetInfo(title_set_info_t * title_set_info) {
	free(title_set_info->title_set);
	free(title_set_info);
}


static void DVDFreeTitlesInfo(titles_info_t * titles_info) {
	free(titles_info->titles);
	free(titles_info);
}


static title_set_info_t* DVDGetFileSet(dvd_reader_t* dvd) {

	/* title interation */
	int title_sets, counter, i;

	/* DVD Video files */
	dvd_stat_t statbuf;

	/* DVD IFO handler */
	ifo_handle_t* vmg_ifo = NULL;

	/* The Title Set Info struct */
	title_set_info_t* title_set_info;

	/* Open main info file */
	vmg_ifo = ifoOpen(dvd, 0);
	if(vmg_ifo == NULL) {
		fprintf( stderr, _("Cannot open Video Manager (VMG) info.\n"));
		return NULL;
	}

	title_sets = vmg_ifo->vmgi_mat->vmg_nr_of_title_sets;

	/* Close the VMG IFO file we got all the info we need */
	ifoClose(vmg_ifo);

	title_set_info = (title_set_info_t*)malloc(sizeof(title_set_info_t));
	if(title_set_info == NULL) {
		perror(PACKAGE);
		return NULL;
	}

	title_set_info->title_set = (title_set_t*)malloc((title_sets + 1) * sizeof(title_set_t));
	if(title_set_info->title_set == NULL) {
		perror(PACKAGE);
		free(title_set_info);
		return NULL;
	}

	title_set_info->number_of_title_sets = title_sets;


	/* Find VIDEO_TS.IFO is present - must be present since we did a ifo open 0 */

	if (DVDFileStat(dvd, 0, DVD_READ_INFO_FILE, &statbuf) != -1) {
		title_set_info->title_set[0].size_ifo = statbuf.size;
	} else {
		DVDFreeTitleSetInfo(title_set_info);
		return NULL;
	}

	/* Find VIDEO_TS.VOB if present*/

	if(DVDFileStat(dvd, 0, DVD_READ_MENU_VOBS, &statbuf) != -1) {
		title_set_info->title_set[0].size_menu = statbuf.size;
	} else {
		title_set_info->title_set[0].size_menu = 0 ;
	}

	/* Take care of the titles which we don't have in VMG */

	title_set_info->title_set[0].number_of_vob_files = 0;
	title_set_info->title_set[0].size_vob[0] = 0;

	if ( verbose > 0 ){
		fprintf(stderr,_("\n\n\nFile sizes for Title set 0 VIDEO_TS.XXX\n"));
		fprintf(stderr,_("IFO = %jd, MENU_VOB = %jd\n"),(intmax_t)title_set_info->title_set[0].size_ifo, (intmax_t)title_set_info->title_set[0].size_menu);
	}

	for(counter = 0; counter < title_sets; counter++) {

		if(verbose > 1) {
			fprintf(stderr,_("At top of loop\n"));
		}


		if(DVDFileStat(dvd, counter + 1, DVD_READ_INFO_FILE, &statbuf) != -1) {
			title_set_info->title_set[counter+1].size_ifo = statbuf.size;
		} else {
			DVDFreeTitleSetInfo(title_set_info);
			return NULL;
		}

		if(verbose > 1) {
			fprintf(stderr,_("After opening files\n"));
		}

		/* Find VTS_XX_0.VOB if present */

		if(DVDFileStat(dvd, counter + 1, DVD_READ_MENU_VOBS, &statbuf) != -1) {
			title_set_info->title_set[counter + 1].size_menu = statbuf.size;
		} else {
			title_set_info->title_set[counter + 1].size_menu = 0 ;
		}

		if(verbose > 1) {
			fprintf(stderr,_("After Menu VOB check\n"));
		}

		/* Find all VTS_XX_[1 to 9].VOB files if they are present */

		i = 0;
		if(DVDFileStat(dvd, counter + 1, DVD_READ_TITLE_VOBS, &statbuf) != -1) {
			for(i = 0; i < statbuf.nr_parts; ++i) {
				title_set_info->title_set[counter + 1].size_vob[i] = statbuf.parts_size[i];
			}
		}
		title_set_info->title_set[counter + 1].number_of_vob_files = i;

		if(verbose > 1) {
			fprintf(stderr,_("After Menu Title VOB check\n"));
		}

		if(verbose > 0) {
			fprintf(stderr,_("\n\n\nFile sizes for Title set %d i.e. VTS_%02d_X.XXX\n"), counter + 1, counter + 1);
			fprintf(stderr,_("IFO: %jd, MENU: %jd\n"), (intmax_t)title_set_info->title_set[counter +1].size_ifo, (intmax_t)title_set_info->title_set[counter +1].size_menu);
			for (i = 0; i < title_set_info->title_set[counter + 1].number_of_vob_files ; i++) {
				fprintf(stderr, _("VOB %d is %jd\n"), i + 1, (intmax_t)title_set_info->title_set[counter + 1].size_vob[i]);
			}
		}

		if(verbose > 1) {
			fprintf(stderr,_("Bottom of loop\n"));
		}
	}

	/* Return the info */
	return title_set_info;
}


int DVDMirror(dvd_reader_t * _dvd, char * targetdir,char * title_name, read_error_strategy_t errorstrat) {

	int i;
	title_set_info_t * title_set_info=NULL;

	title_set_info = DVDGetFileSet(_dvd);
	if (!title_set_info) {
		DVDClose(_dvd);
		return(1);
	}

	for ( i=0; i <= title_set_info->number_of_title_sets; i++) {
		if ( DVDMirrorTitleX(_dvd, title_set_info, i, targetdir, title_name, errorstrat) != 0 ) {
			fprintf(stderr,_("Mirror of Title set %d failed\n"), i);
			DVDFreeTitleSetInfo(title_set_info);
			return(1);
		}
	}
	return(0);
}


int DVDMirrorTitleSet(dvd_reader_t * _dvd, char * targetdir,char * title_name, int title_set, read_error_strategy_t errorstrat) {

	title_set_info_t * title_set_info=NULL;


#ifdef DEBUG
	fprintf(stderr,"In DVDMirrorTitleSet\n");
#endif

	title_set_info = DVDGetFileSet(_dvd);

	if (!title_set_info) {
		DVDClose(_dvd);
		return(1);
	}

	if ( title_set > title_set_info->number_of_title_sets ) {
		fprintf(stderr, _("Cannot copy title_set %d there is only %d title_sets present on this DVD\n"), title_set, title_set_info->number_of_title_sets);
		DVDFreeTitleSetInfo(title_set_info);
		return(1);
	}

	if ( DVDMirrorTitleX(_dvd, title_set_info, title_set, targetdir, title_name, errorstrat) != 0 ) {
		fprintf(stderr,_("Mirror of Title set %d failed\n"), title_set);
		DVDFreeTitleSetInfo(title_set_info);
		return(1);
	}

	DVDFreeTitleSetInfo(title_set_info);
	return(0);
}


int DVDMirrorMainFeature(dvd_reader_t * _dvd, char * targetdir,char * title_name, read_error_strategy_t errorstrat) {

	title_set_info_t * title_set_info=NULL;
	titles_info_t * titles_info=NULL;


	titles_info = DVDGetInfo(_dvd);
	if (!titles_info) {
		fprintf(stderr, _("Guesswork of main feature film failed.\n"));
		return(1);
	}

	title_set_info = DVDGetFileSet(_dvd);
	if (!title_set_info) {
		DVDFreeTitlesInfo(titles_info);
		return(1);
	}

	if ( DVDMirrorTitleX(_dvd, title_set_info, titles_info->main_title_set, targetdir, title_name, errorstrat) != 0 ) {
		fprintf(stderr,_("Mirror of main feature file which is title set %d failed\n"), titles_info->main_title_set);
		DVDFreeTitleSetInfo(title_set_info);
		return(1);
	}

	DVDFreeTitlesInfo(titles_info);
	DVDFreeTitleSetInfo(title_set_info);
	return(0);
}


int DVDMirrorChapters(dvd_reader_t * _dvd, char * targetdir,char * title_name, int start_chapter,int end_chapter, int titles) {


	int result;
	int chapters = 0;
	int i, s;
	int spg, epg;
	int pgc;
	int start_cell, end_cell;
	int vts_title;

	title_set_info_t * title_set_info=NULL;
	titles_info_t * titles_info=NULL;
	ifo_handle_t * vts_ifo_info=NULL;
	int * cell_start_sector=NULL;
	int * cell_end_sector=NULL;

	titles_info = DVDGetInfo(_dvd);
	if (!titles_info) {
		fprintf(stderr, _("Failed to obtain titles information\n"));
		return(1);
	}

	title_set_info = DVDGetFileSet(_dvd);
	if (!title_set_info) {
		DVDFreeTitlesInfo(titles_info);
		return(1);
	}

	if(titles == 0) {
		fprintf(stderr, _("No title specified for chapter extraction, will try to figure out main feature title\n"));
		for (i=0; i < titles_info->number_of_titles ; i++ ) {
			if ( titles_info->titles[i].title_set == titles_info->main_title_set ) {
				if(chapters < titles_info->titles[i].chapters) {
					chapters = titles_info->titles[i].chapters;
					titles = i + 1;
				}
			}
		}
	}

	vts_ifo_info = ifoOpen(_dvd, titles_info->titles[titles - 1].title_set);
	if(!vts_ifo_info) {
		fprintf(stderr, _("Could not open title_set %d IFO file\n"), titles_info->titles[titles - 1].title_set);
		DVDFreeTitlesInfo(titles_info);
		DVDFreeTitleSetInfo(title_set_info);
		return(1);
	}

	vts_title = titles_info->titles[titles - 1].vts_title;

	if (end_chapter > titles_info->titles[titles - 1].chapters) {
		end_chapter = titles_info->titles[titles - 1].chapters;
		fprintf(stderr, _("Truncated the end_chapter; only %d chapters in %d title\n"), end_chapter,titles);
	}

	if (start_chapter > titles_info->titles[titles - 1].chapters) {
		start_chapter = titles_info->titles[titles - 1].chapters;
		fprintf(stderr, _("Truncated the end_chapter; only %d chapters in %d title\n"), end_chapter,titles);
	}



	/* We assume the same PGC for the whole title - this is not true and need to be fixed later on */

	pgc = vts_ifo_info->vts_ptt_srpt->title[vts_title - 1].ptt[start_chapter - 1].pgcn;


	/* Lookup PG for start chapter */

	spg = vts_ifo_info->vts_ptt_srpt->title[vts_title - 1].ptt[start_chapter - 1].pgn;

	/* Look up start cell for this pgc/pg */

	start_cell = vts_ifo_info->vts_pgcit->pgci_srp[pgc - 1].pgc->program_map[spg - 1];


	/* Lookup end cell*/


	if ( end_chapter < titles_info->titles[titles - 1].chapters ) {
		epg = vts_ifo_info->vts_ptt_srpt->title[vts_title - 1].ptt[end_chapter].pgn;
#ifdef DEBUG
		fprintf(stderr,"DVDMirrorChapter: epg %d\n", epg);
#endif

		end_cell = vts_ifo_info->vts_pgcit->pgci_srp[pgc - 1].pgc->program_map[epg -1] - 1;
#ifdef DEBUG
		fprintf(stderr,"DVDMirrorChapter: end cell adjusted %d\n", end_cell);
#endif

	} else {

		end_cell = vts_ifo_info->vts_pgcit->pgci_srp[pgc - 1].pgc->nr_of_cells;
#ifdef DEBUG
		fprintf(stderr,"DVDMirrorChapter: end cell adjusted 2 %d\n",end_cell);
#endif

	}

#ifdef DEBUG
	fprintf(stderr,"DVDMirrorChapter: star cell %d\n", start_cell);
#endif


	/* Put all the cells start and end sector in a dual array */

	cell_start_sector = (int *)malloc( (end_cell - start_cell + 1) * sizeof(int));
	if(!cell_start_sector) {
		fprintf(stderr,_("Memory allocation error 1\n"));
		DVDFreeTitlesInfo(titles_info);
		DVDFreeTitleSetInfo(title_set_info);
		ifoClose(vts_ifo_info);
		return(1);
	}
	cell_end_sector = (int *)malloc( (end_cell - start_cell + 1) * sizeof(int));
	if(!cell_end_sector) {
		fprintf(stderr,_("Memory allocation error\n"));
		DVDFreeTitlesInfo(titles_info);
		DVDFreeTitleSetInfo(title_set_info);
		ifoClose(vts_ifo_info);
		free(cell_start_sector);
		return(1);
	}
#ifdef DEBUG
	fprintf(stderr,"DVDMirrorChapter: start cell is %d\n", start_cell);
	fprintf(stderr,"DVDMirrorChapter: end cell is %d\n", end_cell);
	fprintf(stderr,"DVDMirrorChapter: pgc is %d\n", pgc);
#endif

	for (i=0, s=start_cell; s < end_cell +1 ; i++, s++) {

		cell_start_sector[i] = vts_ifo_info->vts_pgcit->pgci_srp[pgc - 1].pgc->cell_playback[s - 1].first_sector;
		cell_end_sector[i] = vts_ifo_info->vts_pgcit->pgci_srp[pgc - 1].pgc->cell_playback[s - 1].last_sector;
#ifdef DEBUG
		fprintf(stderr,"DVDMirrorChapter: S is %d\n", s);
		fprintf(stderr,"DVDMirrorChapter: start sector %d\n", vts_ifo_info->vts_pgcit->pgci_srp[pgc - 1].pgc->cell_playback[s - 1].first_sector);
		fprintf(stderr,"DVDMirrorChapter: end sector %d\n", vts_ifo_info->vts_pgcit->pgci_srp[pgc - 1].pgc->cell_playback[s - 1].last_sector);
#endif
	}

	bsort_min_to_max(cell_start_sector, cell_end_sector, end_cell - start_cell + 1);

	align_end_sector(cell_start_sector, cell_end_sector,end_cell - start_cell + 1);

#ifdef DEBUG
	for (i=0 ; i < end_cell - start_cell + 1; i++) {
		fprintf(stderr,"DVDMirrorChapter: Start sector is %d end sector is %d\n", cell_start_sector[i], cell_end_sector[i]);
	}
#endif

	result = DVDWriteCells(_dvd, cell_start_sector, cell_end_sector , end_cell - start_cell + 1, titles, title_set_info, titles_info, targetdir, title_name);

	DVDFreeTitlesInfo(titles_info);
	DVDFreeTitleSetInfo(title_set_info);
	ifoClose(vts_ifo_info);
	free(cell_start_sector);
	free(cell_end_sector);

	if( result != 0) {
		return(1);
	} else {
		return(0);
	}
}


int DVDMirrorTitles(dvd_reader_t * _dvd, char * targetdir,char * title_name, int titles) {

	int end_chapter;

	titles_info_t * titles_info=NULL;

#ifdef DEBUG
	fprintf(stderr,"In DVDMirrorTitles\n");
#endif



	titles_info = DVDGetInfo(_dvd);
	if (!titles_info) {
		fprintf(stderr, _("Failed to obtain titles information\n"));
		return(1);
	}


	end_chapter = titles_info->titles[titles - 1].chapters;
#ifdef DEBUG
	fprintf(stderr,"DVDMirrorTitles: end_chapter %d\n", end_chapter);
#endif

	if (DVDMirrorChapters( _dvd, targetdir, title_name, 1, end_chapter, titles) != 0 ) {
		DVDFreeTitlesInfo(titles_info);
		return(1);
	}

	DVDFreeTitlesInfo(titles_info);

	return(0);
}


/**
 * Formats a filesize human readable. For example 25,05 KiB instead of
 * 25648 Bytes.
 */
static void format_filesize(off_t filesize, char* result) {
	char* prefix = "";
	double size = (double)filesize;
	int prefix_count = 0;

	while(size > 1024 && prefix_count < 6) {
		size /= 1024;
		prefix_count++;
	}

	if(prefix_count == 1) {
		prefix = "Ki";
	} else if(prefix_count == 2) {
		prefix = "Mi";
	} else if(prefix_count == 3) {
		prefix = "Gi";
	} else if(prefix_count == 4) {
		prefix = "Ti";
	} else if(prefix_count == 5) {
		prefix = "Pi";
	} else if(prefix_count == 6) {
		prefix = "Ei";
	}

	sprintf(result, "%7.2f %sB", size, prefix);
}


int DVDDisplayInfo(dvd_reader_t* dvd, char* device) {
	int i, f;
	int chapters;
	int channels;
	int titles;
	char title_name[33] = "";
	char size[40] = "";
	title_set_info_t* title_set_info = NULL;
	titles_info_t* titles_info = NULL;

	titles_info = DVDGetInfo(dvd);
	if (!titles_info) {
		fprintf(stderr, _("Guesswork of main feature film failed.\n"));
		return(1);
	}

	title_set_info = DVDGetFileSet(dvd);
	if (!title_set_info) {
		DVDFreeTitlesInfo(titles_info);
		return(1);
	}

	DVDGetTitleName(device, title_name);


	printf(_("DVD-Video information of the DVD with title \"%s\"\n\n"), title_name);

	/* Print file structure */

	printf(_("File Structure DVD\n"));
	printf("VIDEO_TS/\n");
	format_filesize(title_set_info->title_set[0].size_ifo, size);
	printf("\tVIDEO_TS.IFO\t%10jd\t%s\n", (intmax_t)title_set_info->title_set[0].size_ifo, size);

	if (title_set_info->title_set[0].size_menu != 0 ) {
		format_filesize(title_set_info->title_set[0].size_menu, size);
		printf("\tVIDEO_TS.VOB\t%10jd\t%s\n", (intmax_t)title_set_info->title_set[0].size_menu, size);
	}

	for(i = 0 ; i <= title_set_info->number_of_title_sets; i++) {
		format_filesize(title_set_info->title_set[i].size_ifo, size);
		printf("\tVTS_%02i_0.IFO\t%10jd\t%s\n", i, (intmax_t)title_set_info->title_set[i].size_ifo, size);
		if(title_set_info->title_set[i].size_menu != 0) {
			format_filesize(title_set_info->title_set[i].size_menu, size);
			printf("\tVTS_%02i_0.VOB\t%10jd\t%s\n", i, (intmax_t)title_set_info->title_set[i].size_menu, size);
		}
		if(title_set_info->title_set[i].number_of_vob_files != 0) {
			for(f = 0; f < title_set_info->title_set[i].number_of_vob_files; f++) {
				format_filesize(title_set_info->title_set[i].size_vob[f], size);
				printf("\tVTS_%02i_%i.VOB\t%10jd\t%s\n", i, f + 1, (intmax_t)title_set_info->title_set[i].size_vob[f], size);
			}
		}
	}

	printf(_("\n\nMain feature:\n"));
	printf(_("\tTitle set containing the main feature is %d\n"), titles_info->main_title_set);
	for (i=0; i < titles_info->number_of_titles ; i++ ) {
		if (titles_info->titles[i].title_set == titles_info->main_title_set) {
			if(titles_info->titles[i].aspect_ratio == 3) {
				printf(_("\tThe aspect ratio of the main feature is 16:9\n"));
			} else if (titles_info->titles[i].aspect_ratio == 0) {
				printf(_("\tThe aspect ratio of the main feature is 4:3\n"));
			} else {
				printf(_("\tThe aspect ratio of the main feature is unknown\n"));
			}

			printf(ngettext("\tThe main feature has %d angle\n",
					"\tThe main feature has %d angles\n",
					titles_info->titles[i].angles), titles_info->titles[i].angles);
			printf(ngettext("\tThe main feature has %d audio track\n",
					"\tThe main feature has %d audio tracks\n",
					titles_info->titles[i].audio_tracks), titles_info->titles[i].audio_tracks);
			printf(ngettext("\tThe main feature has %d subpicture channel\n",
					"\tThe main feature has %d subpicture channels\n",
					titles_info->titles[i].sub_pictures), titles_info->titles[i].sub_pictures);
			chapters=0;
			channels=0;

			for (f=0; f < titles_info->number_of_titles ; f++ ) {
				if ( titles_info->titles[i].title_set == titles_info->main_title_set ) {
					if(chapters < titles_info->titles[f].chapters) {
						chapters = titles_info->titles[f].chapters;
					}
					if(channels < titles_info->titles[f].audio_channels) {
						channels = titles_info->titles[f].audio_channels;
					}
				}
			}
			printf(ngettext("\tThe main feature has a maximum of %d chapter in one of its titles\n",
					"\tThe main feature has a maximum of %d chapters in one of its titles\n",
					chapters), chapters);
			printf(ngettext("\tThe main feature has a maximum of %d audio channel in one of its titles\n",
					"\tThe main feature has a maximum of %d audio channels in one of its titles\n",
					channels), channels);
			break;
		}
	}

	printf(_("\n\nTitle Sets:"));
	for (f=0; f < title_set_info->number_of_title_sets ; f++ ) {
		printf(_("\n\n\tTitle set %d\n"), f + 1);
		for (i=0; i < titles_info->number_of_titles ; i++ ) {
			if (titles_info->titles[i].title_set == f + 1) {
				if(titles_info->titles[i].aspect_ratio == 3) {
					printf(_("\t\tThe aspect ratio of title set %d is 16:9\n"), f + 1);
				} else if (titles_info->titles[i].aspect_ratio == 0) {
					printf(_("\t\tThe aspect ratio of title set %d is 4:3\n"), f + 1);
				} else {
					printf(_("\t\tThe aspect ratio of title set %d is unknown\n"), f + 1);
				}
				printf(ngettext("\t\tTitle set %d has %d angle\n",
						"\t\tTitle set %d has %d angles\n",
						titles_info->titles[i].angles), f + 1, titles_info->titles[i].angles);
				printf(ngettext("\t\tTitle set %d has %d audio track\n",
						"\t\tTitle set %d has %d audio tracks\n",
						titles_info->titles[i].audio_tracks), f + 1, titles_info->titles[i].audio_tracks);
				printf(ngettext("\t\tTitle set %d has %d subpicture channel\n",
						"\t\tTitle set %d has %d subpicture channels\n",
						titles_info->titles[i].sub_pictures), f + 1, titles_info->titles[i].sub_pictures);
				break;
			}
		}

		titles = 0;
		for(i = 0; i < titles_info->number_of_titles; i++) {
			if (titles_info->titles[i].title_set == f + 1) {
				titles++;
			}
		}
		printf(ngettext("\n\t\tTitle included in title set %d is\n",
				"\n\t\tTitles included in title set %d are\n",
				titles), f + 1);

		for (i=0; i < titles_info->number_of_titles ; i++ ) {
			if (titles_info->titles[i].title_set == f + 1) {
				printf(_("\t\t\tTitle %d:\n"), i + 1);
				printf(ngettext("\t\t\t\tTitle %d has %d chapter\n",
						"\t\t\t\tTitle %d has %d chapters\n",
						titles_info->titles[i].chapters), i + 1, titles_info->titles[i].chapters);
				printf(ngettext("\t\t\t\tTitle %d has %d audio channel\n",
						"\t\t\t\tTitle %d has %d audio channels\n",
						titles_info->titles[i].audio_channels), i + 1, titles_info->titles[i].audio_channels);
			}
		}
	}
	DVDFreeTitlesInfo(titles_info);
	DVDFreeTitleSetInfo(title_set_info);

	return(0);
}
