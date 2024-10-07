
#include "MemoryConstants.h"
#include "PhysicalMemory.h"



word_t min(word_t a, word_t b)
{
    return a < b ? a : b;
}
word_t max(word_t a, word_t b)
{
    return a > b ? a : b;
}

word_t abs(word_t num)
{
    return (num < 0) ? -num : num;
}

void init_root_frame()
{
    for (uint64_t i = 0; i < PAGE_SIZE; ++i)
    {
        PMwrite(i, 0);
    }
}

void VMinitialize()
{
    init_root_frame();
}


void clear_frame(uint64_t frame_index)
{
    for(uint64_t i = 0; i < PAGE_SIZE; i++)
    {
        PMwrite(frame_index * PAGE_SIZE + i, 0);
    }
}

/**
 *
 * @param frame_index - the current frame index of the recursion
 * @param curr_depth - the recursion layer_index
 * @param curr_frame
 * @param previous_frame - the frame previous to the curr frame
 * This function unlinks the empty frame that was found from its father-frame
 */

void disconnect_father_first_case(uint64_t frame_index, uint64_t curr_depth,
                                  uint64_t curr_frame, uint64_t previous_frame)
{
    if (curr_depth == TABLES_DEPTH)
    {
        return;
    }
    if(curr_depth != 0)
    {
        previous_frame = curr_frame;
    }
    for (int i = 0; i < PAGE_SIZE; i++)
    {
        word_t val = 0;
        PMread(curr_frame * PAGE_SIZE + i, &val);
        if ((uint64_t)val == frame_index)  // disconnect it from the previous frame
        {
            PMwrite(previous_frame * PAGE_SIZE + i, 0);
            return;
        }
        if (val != 0)
        {
            disconnect_father_first_case(frame_index, curr_depth + 1,
                                         (uint64_t)val, previous_frame);
        }
    }

}

/**
 *
 * @param page_to_evict - the page to be evicted
 * This function gets the page to be evicted and unlinks it from
 * the father-frame that was pointing to the frame of page_to_evict
 */

void disconnect_father_evict(uint64_t page_to_evict)
{
    word_t val = 0;
    for(int i = TABLES_DEPTH - 2; i >= 0; i--)
    {
        PMread(val * PAGE_SIZE +
               ((page_to_evict >> (i + 1) * OFFSET_WIDTH) % PAGE_SIZE),
               &val);
    }
    auto make_shorter = val * PAGE_SIZE +
                        (page_to_evict % PAGE_SIZE);
    PMwrite(make_shorter, 0);
}

/**
 *
 * @param frame_index - the current frame index of the recursion
 * @param max_frame_index - the maximal frame index that we've reached
 * @param layer_index
 * @param parent_frame - the previous frame that we've been to
 * @param empty_frame_found - a boolean indicating that we found an empty frame
 * @param final_frame
 * At the end of this function final frame will hold the number of an empty
 * frame (if there is no empty frame we will later ignore its value)
 */

void find_frame(uint64_t frame_index, uint64_t& max_frame_index,
                uint64_t layer_index, uint64_t parent_frame,
                bool& empty_frame_found, uint64_t& final_frame)
{
    if (layer_index == TABLES_DEPTH)
    {
        return;
    }

    word_t val = 0;
    bool is_empty = true;

    for (uint64_t i = 0; i < PAGE_SIZE; i++)
    {
        uint64_t frame_add = frame_index * PAGE_SIZE;
        PMread(frame_add + i, &val);

        if (val != 0)
        {
            is_empty = false;
            max_frame_index = max((word_t)max_frame_index, val);
            find_frame(val, max_frame_index,
                       layer_index + 1,
                       parent_frame, empty_frame_found,
                       final_frame);
            if(empty_frame_found)
            {
                return;
            }
        }
    }

    if(is_empty && parent_frame != frame_index && !empty_frame_found)
    {
        final_frame = frame_index;
        empty_frame_found = true;
        return;
    }

}

/**
 *
 * @param frame_index - the current frame index of the recursion
 * @param frame_of_page_to_evict - a reference of the page to be evicted
 * @param max_cyc_dis - the maximum cyclical distance so far
 * @param page_to_evict - a reference of the page to be evicted
 * @param layer_index
 * @param curr_path
 * @param page_swapped_in - virtual address >> offset width
 * At the end of this function page_to_evict and frame_of_page_to_evict
 * will hold the relevant values
 */

void find_page_to_evict(uint64_t frame_index, uint64_t& frame_of_page_to_evict,
                        uint64_t& max_cyc_dis,
                        uint64_t& page_to_evict, uint64_t layer_index,
                        uint64_t curr_path, uint64_t page_swapped_in)
{
    if(layer_index == TABLES_DEPTH) // reached a leaf
    {
        // Cyclical calculation
        uint64_t curr_cyc_dis =
                min(NUM_PAGES - abs((word_t)curr_path -
                                    (word_t)page_swapped_in),
                    abs((word_t) curr_path - (word_t)page_swapped_in));
        if (curr_cyc_dis > max_cyc_dis)
        {
            max_cyc_dis = curr_cyc_dis;
            page_to_evict = curr_path;
            frame_of_page_to_evict = frame_index;
        }
        return;

    }
    word_t val = 0;
    for (uint64_t i = 0; i < PAGE_SIZE; ++i)
    {

        uint64_t frame_add = frame_index * PAGE_SIZE;
        PMread(frame_add + i, &val);

        if (val != 0)
        {
            find_page_to_evict(val, frame_of_page_to_evict,
                               max_cyc_dis,
                               page_to_evict, layer_index + 1,
                               (curr_path << OFFSET_WIDTH) +  i,
                               page_swapped_in);
        }
    }

}


/**
 *
 * @param virtualAddress
 * @return This function returns the physical address of a given virtual one
 */

uint64_t convert_to_pm(uint64_t virtualAddress)
{
    uint64_t offset = virtualAddress % PAGE_SIZE;   // calculation of offset
    uint64_t page_swapped_in = virtualAddress >> OFFSET_WIDTH;

    // **** initialization variables ***
    uint64_t curr_address = 0;
    word_t addr1 = 0, addr2 = 0;
    uint64_t frame_index = 0, max_cyc_dis = 0, frame_of_page_to_evict = 0;
    uint64_t page_to_evict = 0, found_frame = 0, final_frame = 0;
    uint64_t max_frame_index = 0;
    bool should_restore = false, empty_frame_found = false;
    // **** initializations variables ***

    for(int i = TABLES_DEPTH - 1; i >= 0; i--)
    {
        // *** initialization of referenced values ***
        empty_frame_found = false;
        final_frame = 0;
        max_frame_index = 0;
        page_to_evict = 0;
        frame_of_page_to_evict = 0;
        max_cyc_dis = 0;
        // *** initialization of referenced values ***

        curr_address = virtualAddress >> ((i + 1) * OFFSET_WIDTH);
        curr_address = curr_address % PAGE_SIZE;
        PMread(addr2 * PAGE_SIZE + curr_address, &addr1);
        if(addr1 == 0)  // find an unused frame or evict a page....
        {
            find_frame(0, max_frame_index, 0,
                       addr2, empty_frame_found, final_frame);

            // First case - an empty frame was found
            if(empty_frame_found)
            {
                found_frame = final_frame;
                disconnect_father_first_case( found_frame,
                                              0, 0,
                                              0);
            }

            // Second case - The maximal frame index is smaller than NUM_FRAMES
            else if (max_frame_index + 1 < NUM_FRAMES )
            {

                found_frame = max_frame_index + 1;
                clear_frame(found_frame);
            }
            else     // Third case - evict a page
            {
                // find a page to evict and its frame
                find_page_to_evict(frame_index,frame_of_page_to_evict
                                   ,max_cyc_dis, page_to_evict,
                                   0, 0, page_swapped_in);
                found_frame = frame_of_page_to_evict;
                // unlink the
                disconnect_father_evict(page_to_evict);
                PMevict(frame_of_page_to_evict,
                        page_to_evict);
                clear_frame(found_frame);
            }
            should_restore = true;
            PMwrite(addr2 * PAGE_SIZE + curr_address,
                    (word_t)found_frame);
            addr1 = word_t (found_frame);
        }
        addr2 = addr1;
    }
    if(should_restore)
    {
        PMrestore(found_frame, page_swapped_in);
    }
    return addr1 * PAGE_SIZE + offset;
}



int VMwrite(uint64_t virtualAddress, word_t value)
{
    if(virtualAddress >= VIRTUAL_MEMORY_SIZE)
    {
        return 0;
    }

    uint64_t physical_address = convert_to_pm(virtualAddress);

    PMwrite(physical_address, value);
    return 1;
}


int VMread(uint64_t virtualAddress, word_t* value)
{
    if(virtualAddress >= VIRTUAL_MEMORY_SIZE)
    {
        return 0;
    }

    uint64_t physical_address = convert_to_pm(virtualAddress);
    PMread(physical_address, value);
    return 1;
}


