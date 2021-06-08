# External Merge Sort

Efficient implementation of external merge sort for embedded devices. This version is specifically for the Arduino.

The external merge sort has the following benefits:

1. Minimum memory usage is only three page buffers. 
2. No use of dynamic memory (i.e. malloc()). 
3. Easy to use and include in existing projects. 
4. Open source license. Free to use for commerical and open source projects.

## License
[![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)

## Code Files

* external_merge_sort_iterator_block.c, external_merge_sort_iterator_block.h - implementation of external merge sort
* test_external_merge_sort_block.c - test file
* in_memory_sort.c, in_memory_sort.h - implementation of quick sort
* serial_c_interface.c, serial_c_interface.h - serial output for Arduino
* ion_file.c, ion_file.h - file abstraction for files on SD card

#### Ramon Lawrence<br>University of British Columbia Okanagan
