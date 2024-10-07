# Virtual Memory Implementation

## 📚 Overview
This project implements a virtual memory interface using **hierarchical page tables** of arbitrary depth. The simulation includes both **virtual address translation** and **physical memory** management, following the paging model where logical addresses are mapped to physical addresses.

### 🔍 Virtual Memory Recap
Virtual memory allows processes to use more memory than is available by mapping virtual addresses into physical ones. Since the physical memory is limited, not all of the virtual memory space can fit in at the same time, meaning parts of it must be stored on the hard drive.

In this project:
- **Pages** represent blocks in the virtual memory.
- **Frames** represent blocks in the physical memory.
- **Paging** swaps pages in and out between the RAM and hard drive, maintaining performance and space efficiency.

### 🌲 Hierarchical Page Tables
To avoid waste in physical memory, we use **hierarchical page tables**:
- These tables divide address translation into multiple layers, stored in frames like any other pages.
- The depth of the page table tree is determined by the memory configuration, and translation occurs in multiple steps.

For example, translating the address `[101][0001][0110]` with 2 layers follows these steps:

1. **First translation**:
   ```cpp
   PMread(0 + 5, &addr1)
   ```

2. **Second translation**:
   ```cpp
   PMread(addr1 * PAGE_SIZE + 1, &addr2)
   ```

3. **Write to memory**:
   ```cpp
   PMwrite(addr2 * PAGE_SIZE + 6, value)
   ```

### 🔧 Task Overview
Your task is to implement the virtual memory API, as defined in `VirtualMemory.h`. You will use the physical memory API from `PhysicalMemory.h`, which simulates the physical memory. 

Some key constraints:
- No global variables or dynamic memory allocation (e.g., no `std::vector`, `std::map`).
- The root page table is always in frame 0 of the physical memory.
- Address translation must work for arbitrary page table depths.

### 💡 Design Details
- The root page table starts in frame 0, which never gets evicted.
- Each row of the table fits into one word.
- Pages that are not currently in physical memory trigger a **page fault** and must be swapped back in from the disk.

When a frame is needed, we find it by:
1. Searching for an **empty table**.
2. Looking for an **unused frame**.
3. If all frames are used, **evicting a page** based on a cyclical distance metric.
