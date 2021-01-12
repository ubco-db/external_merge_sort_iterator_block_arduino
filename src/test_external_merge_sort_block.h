/******************************************************************************/
/**
@file		test_external_merge_sort_block.c
@author		Riley Jackson, Ramon Lawrence
@brief		This file does performance/correctness testing of external merge sort.
@copyright	Copyright 2020
			The University of British Columbia,
			IonDB Project Contributors (see AUTHORS.md)
@par Redistribution and use in source and binary forms, with or without
	modification, are permitted provided that the following conditions are met:

@par 1.Redistributions of source code must retain the above copyright notice,
	this list of conditions and the following disclaimer.

@par 2.Redistributions in binary form must reproduce the above copyright notice,
	this list of conditions and the following disclaimer in the documentation
	and/or other materials provided with the distribution.

@par 3.Neither the name of the copyright holder nor the names of its contributors
	may be used to endorse or promote products derived from this software without
	specific prior written permission.

@par THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
	AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
	IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
	ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
	LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
	CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
	SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
	INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
	CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
	ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
	POSSIBILITY OF SUCH DAMAGE.
*/
/******************************************************************************/
#include <time.h>
#include <string.h>

#include "external_merge_sort_iterator_block.h"
#include "in_memory_sort.h"

#define EXTERNAL_SORT_MAX_RAND 1000000

/**
 * Generates random test data records
 */ 
int
external_sort_write_int32_random_data(
	ION_FILE *unsorted_file,
	int32_t num_values,
	int32_t record_size)
{
	printf("Random Data: %d\n", num_values);	

	int32_t i;
	test_record_t buf;

	/* Data record is empty. Only need to reset to 0 once as reusing struct. */		
	for (int k = 0; k < record_size-4; k++)
	{
		buf.value[k] = 0;
	}

	for (i = 0; i < num_values; i++)
	{
	 	/* Generate random key */
        buf.key = rand() % EXTERNAL_SORT_MAX_RAND;	

		if (0 == fwrite(&buf, record_size, 1, unsorted_file))
		{
			return 10;
		}        
	}

	return 0;
}

/**
 * Generates increasing or decreasing test data records
 */
int
external_sort_write_int32_sequential_data(
	ION_FILE *unsorted_file,
	int32_t num_values,
	int32_t record_size,
	int reverse)
{	
	int32_t i;
	test_record_t buf;
	
	/* Data record is empty. Only need to reset to 0 once as reusing struct. */    	
	for (int k = 0; k < record_size-4; k++)
	{
		buf.value[k] = 0;
	}

	for (i = 0; i < num_values; i++)
	{		
		if (reverse)
		{
			buf.key = num_values - i;
		}
		else
		{
			buf.key = i + 1;
		}	

		if (0 == fwrite(&buf, record_size, 1, unsorted_file))
		{
			return 10;
		}
	}

	return 0;
}


/**
 * Iterates through records in a file returning NULL when no more records.
 */
int fileRecordIterator(void* state, void* buffer)
{
	file_iterator_state_t* fileState = (file_iterator_state_t*) state;

	if (fileState->recordsRead >= fileState->totalRecords)
		return 0;

	/* Read next record */
    /* TODO: Improve by reading a block at a time */
	fread(buffer, fileState->recordSize, 1, fileState->file);
	fileState->recordsRead++;
	return 1;
}

/**
 * Runs all tests and collects benchmarks
 */ 
void runalltests_external_merge_sort()
{
	int8_t          numRuns = 5;
    metrics_t       metric[numRuns];
    external_sort_t es;

    /* Set random seed */
    int seed = time(0);  
    seed = 2020;  
    printf("Seed: %d\n", seed);
    srand(seed);     

 	int mem;
    for(mem = 3; mem <= 3; mem++) 
    {
        printf("<---- New Tests M=%d ---->\n", mem);
        int t;
        for (t = 5; t < 6; t++) 
        {
            printf("--- Test Number %d ---\n", t);
            for (int r=0; r < numRuns; r++)
            {            
                printf("--- Run Number %d ---\n", (r+1));
                int buffer_max_pages = mem;
                    
                metric[r].num_reads = 0;
                metric[r].num_writes = 0;
                metric[r].num_compar = 0;
                metric[r].num_memcpys = 0;                

                es.key_size = sizeof(int32_t); 
                es.value_size = 12;
                es.headerSize = BLOCK_HEADER_SIZE;
                es.record_size = es.key_size + es.value_size;
                es.page_size = 512;

                int32_t values_per_page = (es.page_size - es.headerSize) / es.record_size;
                int32_t num_test_values = values_per_page;
                
                int k;
                for (k = 0; k < t; k++)
                {                   
                    if (k == 0) 
                        num_test_values *= buffer_max_pages;                    
                    else
                        num_test_values *= 2;                    
                }
                /* Add variable number of records so pages not completely full (optional) */
                // num_test_values += rand() % 10;
                es.num_pages = (uint32_t) (num_test_values + values_per_page - 1) / values_per_page; 
                es.compare_fcn = merge_sort_int32_comparator;

                /* Buffers and file offsets used by sorting algorithim*/                
                long result_file_ptr;
                char *buffer = (char*) malloc((size_t) buffer_max_pages * es.page_size + es.record_size);
                char *tuple_buffer = buffer + es.page_size * buffer_max_pages;
                if (NULL == buffer) {
                    printf("Error: Out of memory!\n");
                    return;
                }

                /* Create the file and fill it with test data */
                ION_FILE *fp;
                fp = fopen("myfile.bin", "w+b");
                if (NULL == fp) {
                    printf("Error: Can't open file!\n");
                    return;
                }

               	// external_sort_write_int32_sequential_data(fp, num_test_values, es.record_size, 0);
				external_sort_write_int32_random_data(fp, num_test_values, es.record_size);

                fflush(fp);                
                fseek(fp, 0, SEEK_SET);

                file_iterator_state_t iteratorState;
                iteratorState.file = fp;
                iteratorState.recordsRead = 0;
                iteratorState.totalRecords = num_test_values;
                iteratorState.recordSize = es.record_size;

                /* Open output file */
                ION_FILE *outFilePtr;

                outFilePtr = fopen("tmpsort.bin", "w+b");

                if (NULL == outFilePtr)
                {
                    printf("Error: Can't open output file!\n");			
                }

                /* Run and time the algorithim */
                printf("num test values: %li\n", num_test_values);
                printf("blocks:%li\n", es.num_pages);
                #if defined(ARDUINO)
                unsigned long startMillis = millis(); /* initial start time */
                #else
                clock_t start = clock();
                #endif                    

               	int err = extern_merge_sort_iterator_block(&fileRecordIterator, &iteratorState, &tuple_buffer, outFilePtr, buffer, buffer_max_pages, &es, &result_file_ptr, &metric[r], merge_sort_int32_comparator);	

                if (8 == err) {
                    printf("Out of memory!\n");
                } else if (10 == err) {
                    printf("File Read Error!\n");
                } else if (9 == err) {
                    printf("File Write Error!\n");
                    result_file_ptr = 0;
                }

                #if defined(ARDUINO)
                unsigned long duration = millis() - startMillis; /* initial start time */
                
                printf("Elapsed Time: %lu s\n", duration);
                metric[r].time = duration;
                #else
                clock_t end = clock();
                printf("Elapsed Time: %0.6f s\n", ((double) (end - start)) / CLOCKS_PER_SEC);
                metric[r].time = ((double) (end - start)) / CLOCKS_PER_SEC;
                #endif

                /* Verify the data is sorted*/
                int sorted = 1;                
                fp = outFilePtr;
                char *rec_last = (char*) malloc(es.record_size);
                memcpy(rec_last, buffer + es.headerSize, es.record_size);

                fseek(fp, result_file_ptr, SEEK_SET);

                uint32_t i;
                test_record_t last, *buf;
                int32_t numvals = 0;

                /* Read blocks of output file to check if sorted */
                for (i=0; i < es.num_pages; i++)
                {
                    if (0 == fread(buffer, es.page_size, 1, outFilePtr))
                    {	printf("Failed to read block.\n");
                        sorted = 0;
                    }

                    /* Read records from file */
                    int count = *((int16_t*) (buffer+BLOCK_COUNT_OFFSET));
                    /* printf("Block: %d Count: %d\n", *((int16_t*) buffer), count); */
                    void* addr = &(buffer[0]);
           
                    for (int j=0; j < count; j++)
                    {	
                        buf = (test_record_t*) (buffer+es.headerSize+j*es.record_size);				
                        numvals++;
                        #ifdef DATA_COMPARE
                        if (sampleData[numvals-1] != buf->key)
                        {
                            printf("Num: %d ",numvals);   
                            printf(" \tExpected: %li",sampleData[numvals-1]);   
                            printf(" \tActual: %li\n",buf->key);   
                            sorted = 0;                     
                        }
                        #endif
                        
                       if (i > 0 && last.key > buf->key)
                        {
                            sorted = 0;
                            printf("VERIFICATION ERROR Offset: %li",ftell(outFilePtr)-es.page_size);
                            printf(" Block header: %d",*((int32_t*) addr));
                            printf(" Records: %d",*((int16_t*) (addr+BLOCK_COUNT_OFFSET)));
                            printf(" Record key: %li\n", ((test_record_t*) (addr+BLOCK_HEADER_SIZE))->key);
                            printf("%li not less than %li\n", last.key, buf->key);
                        }

                        memcpy(&last, buf, es.record_size);				
                    }
                    /* Need to preserve buf between page loads as buffer is repalced */
                }		

                if (numvals != num_test_values)
                {
                    printf("ERROR: Missing values: %d\n", (num_test_values-numvals));
                    sorted = 0;
                };

                /* Print Results*/
                printf("Sorted: %d\n", sorted);
                printf("Reads:%li\n", metric[r].num_reads);
                printf("Writes:%li\n", metric[r].num_writes);
                printf("I/Os:%li\n\n", metric[r].num_reads + metric[r].num_writes);
                printf("Num Comparisons:%li\n", metric[r].num_compar);
                printf("Num Memcpys:%li\n", metric[r].num_memcpys);
                /* printf("Num Runs:%li\n", metric[r].num_runs); */

                /* Clean up and print final result*/
                free(buffer);
                if (0 != fclose(fp)) {
                    printf("Error file not closed!");
                }
                if (sorted)
                    printf("SUCCESS");
                else
                    printf("FAILURE");
                printf("\n\n");

            }
            /* Print Average Results*/
            int32_t value = 0;
            double v = 0;
            int32_t vals[5];
            printf("Time:\t\t");
            for (int i=0; i < numRuns; i++)
            {                
               printf("%li\t", (long) (metric[i].time));
               v+= metric[i].time;
            }
            printf("%li\n", (long) (v/numRuns));
            vals[0] = (long) (v/numRuns);
            printf("Reads:\t\t");
            for (int i=0; i < numRuns; i++)
            {                
            printf("%li\t", metric[i].num_reads);
            value += metric[i].num_reads;
            }
            printf("%li\n", value/numRuns);
            vals[1] = value/numRuns;
            value = 0;
            printf("Writes: \t");
            for (int i=0; i < numRuns; i++)
            {                
                printf("%li\t", metric[i].num_writes);
                value += metric[i].num_writes;
            }
            printf("%li\n", value/numRuns);
            vals[2] = value/numRuns;
            value = 0;
            printf("Compares: \t");
            for (int i=0; i < numRuns; i++)
            {                
            printf("%li\t", metric[i].num_compar);
            value += metric[i].num_compar;
            }
            printf("%li\n", value/numRuns);
            vals[3] = value/numRuns;
            value = 0;
            printf("Copies: \t");
            for (int i=0; i < numRuns; i++)
            {                
                printf("%li\t", metric[i].num_memcpys);
                value += metric[i].num_memcpys;
            }
            printf("%li\n", value/numRuns);   
            vals[4] = value/numRuns;        
            printf("%li\t%li\t%li\t%li\t%li\n",vals[0], vals[1], vals[2], vals[3], vals[4]);
        }
    }
}
