/******************************************************************************/
/**
@file		external_merge_sort_iterator_block.c
@author		Riley Jackson, Ramon Lawrence
@brief		File-based external merge sort supporting block header and replacement
			selection.
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
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <math.h>

#include "external_merge_sort_iterator_block.h"
#include "in_memory_sort.h"

/*
#define DEBUG  1
*/

/**
@brief     	External merge sort with input iterator and supporting variable number of records per block.
@param      iterator
                Row iterator for reading input rows
@param      iteratorState
                Structure stores state of iterator (file info etc.)
@param      tupleBuffer
                Pre-allocated space to store one tuple (row) of input being sorted
@param      file
                Already opened file to store sorting output (and in-progress temporary results)
@param      buffer
                Pre-allocated space used by algorithm during sorting
@param      bufferSizeInBlocks
                Size of buffer in blocks
@param      es
                Sorting state info (block size, record size, etc.)
@param      resultFilePtr
                Offset within output file of first output record
@param      metric
                Tracks algorithm metrics (I/Os, comparisons, memory swaps)
@param      compareFn
                Record comparison function for record ordering
*/
int extern_merge_sort_iterator_block(
	int (*iterator)(void *state, void* buffer),
	void	*iteratorState,
	void	*tupleBuffer,
	ION_FILE *file,
	char 	*buffer,
	int 	bufferSizeInBlocks,
	external_sort_t *es,
	long 	*resultFilePtr,
	metrics_t *metric,
	int8_t (*compareFn)(void *a, void *b))
{
	printf("External merge sort iterator version with blocks and file overwrite.\n");

	int8_t 		subListsInRun = 0;	 	  		
	int16_t 	tuplesPerPage = (es->page_size - es->headerSize) / es->record_size;

	/* create initial sorted sublists (size of M) */
	long 		lastWritePos = 0;
	int32_t 	numRecordsRead = bufferSizeInBlocks * tuplesPerPage;
	int 		i=0, status;
	int32_t 	numSublist=0;
	int8_t 		passNumber = 1;

	test_record_t *tuple, *value;
	void *		addr;
	int32_t 	lowId;
	int32_t 	numblocks = 0;
	size_t 		bufferOutputPos; /* points to next empty tuple position in buffer block */ // Start after header - not at 0
	
	do
	{		
		i = 0;
		/* Fill up buffer with input records from iterator */
		addr = buffer+es->headerSize;
		while (i < numRecordsRead)
		{		
			status=iterator(iteratorState, addr);
			if (status == 0)			
				break;
			
			i++;
			addr += es->record_size;                    /* Read a record. Advance to next record location in buffer. */
			
		}
		if (i == 0)
			break;

		numRecordsRead = i;
		int pageio = (numRecordsRead + tuplesPerPage - 1) / tuplesPerPage;  
		metric->num_reads += pageio;
		
		/* Sort in memory and write to output file */
		in_memory_sort(buffer+es->headerSize, (uint32_t)numRecordsRead, es->record_size, compareFn, 1);			

		/* Write to output file */
		fseek(file, lastWritePos, SEEK_SET);	
		int lastOffset = 0;	
		for (i=0; i < pageio-1; i++)
		{
			/* Setup block header */
			addr = buffer + lastOffset;
			*((int32_t*) addr) = i;		                                                        /* Block index */
			*((int16_t*) (addr+BLOCK_COUNT_OFFSET)) = tuplesPerPage;		                    /* Block record count */

			if (0 == fwrite(addr, es->page_size, 1, file))
				return 9;			
                 
			lastOffset += es->record_size * tuplesPerPage;
		}
		/* Write last page */
		addr = buffer + lastOffset;
		*((int32_t*) addr) = i;		                                                            /* Block index */
		*((int16_t*) (addr+BLOCK_COUNT_OFFSET)) = numRecordsRead - tuplesPerPage * i;		    /* Block record count */

		if (0 == fwrite(addr, es->page_size, 1, file))	 
			return 9;	
		
		lastWritePos = ftell(file);
		metric->num_writes += pageio;	
		numSublist++;
	} while (status == 1);
	
	if (numSublist == 1)
	{	/* No merge phase necessary */
		*resultFilePtr = 0;
		return 0;
	}

	/* Merge phase: recursively combine M-1 sublists */
	int8_t maxSublistsInRun = bufferSizeInBlocks - 1;

	/* Allocate file position arrays */
	int32_t 	*runOffset = (int32_t*) malloc(sizeof(int32_t) * maxSublistsInRun);   	/* Offset of run in file/memory */
	int32_t 	*runCount = (int32_t*) malloc(sizeof(int32_t) * maxSublistsInRun); 	 	/* Number of blocks in run  */
	long 		ptrLastBlock=lastWritePos-es->page_size, ptrFirstBlock=0, ptrNextFirst=lastWritePos;
	int32_t 	blockIndex;
	int32_t 	firstPartitionSize = maxSublistsInRun; 									/* TODO: Will change with replacement selection */
	int 		newPass = 1;	
	int32_t 	*sublsTuplePos = (int32_t*) malloc(sizeof(int32_t) * maxSublistsInRun); /* current tuple of block being read */

	/* Verify memory was allocated for sublist pointer arrays */
	if (NULL == sublsTuplePos)
	{				
		free(runOffset);
		free(runCount);		
		return 8;
	}
		
	printf("Starting new merge pass: %d. Sublists: %d  First offset: %li  Last offset: %li  Next first offset: %li\n", passNumber, numSublist, ptrFirstBlock, ptrLastBlock, ptrNextFirst);							

	while (numSublist > 1)
	{
		/* Fill file position arrays */
		for (i=0; i < maxSublistsInRun && i < numSublist; i++)
		{		
			/* Check if have processed all input  */
			if (ptrLastBlock < ptrFirstBlock)
			{					
				newPass = 1;			
							
				if (i > 0 && i < maxSublistsInRun-1) 
				{	/* Merge with first sublist in run rather than last one	*/									
					/* Update first and last block pointers to include current output run and exclude first run that is already merged */				
					ptrFirstBlock = ptrNextFirst + firstPartitionSize * es->page_size;  // Skip first partition as using it				
					printf("Merging first block in prior run with first block in next run.\n");
				}	
				else
				{	/* Merge with sublists already generated in this pass */
					ptrFirstBlock = ptrNextFirst;									
				}				
				
				ptrLastBlock = lastWritePos - es->page_size;				/* Must set ptrLastBlock before changed lastWritePos */
				passNumber++;
				if (passNumber % 3 == 0)
				{	/* Starting writing at the beginning of the file/memory again every 3rd pass */
					lastWritePos = 0;
					printf("Wrapping to write at start of file/memory.\n");
				}
				ptrNextFirst = lastWritePos;

				printf("Starting new merge pass: %d. Sublists: %d  First offset: %li  Last offset: %li  Next first offset: %li\n", passNumber, numSublist, ptrFirstBlock, ptrLastBlock, ptrNextFirst);							
			}

			/* Read page at last write position and calculate start of run based on block index */
			fseek(file, ptrLastBlock, SEEK_SET);

			if (0 == fread(&buffer[0], es->page_size, 1, file))		
				return 10;
			metric->num_reads += 1;

			/* Retrieve block index */
			blockIndex = *((int32_t*) buffer);		

			runCount[i] = blockIndex+1;
			runOffset[i] = ptrLastBlock - blockIndex*es->page_size;			
			sublsTuplePos[i] = 0;
		
			#if defined(DEBUG)
				printf("MERGE Count: %d Offset: %d Block header: %d  Records: %d  First record: %p  Record key: %d\n",runCount[i], runOffset[i], *((int32_t*) buffer), *((int16_t*) (buffer+4)), (buffer+6), ((test_record_t*) (buffer+6))->key);
			#endif

			/* Adjust last block pointer to last block in next sublist */
			ptrLastBlock = runOffset[i] - es->page_size;									
		}
		subListsInRun = i;

		if (newPass)
		{
			int32_t newFirstPartitionSize = runCount[0]+runCount[1];
			#if defined(DEBUG)
				printf("Size of first partition: %d\n", newFirstPartitionSize);
			#endif
			firstPartitionSize = newFirstPartitionSize;
			newPass = 0;
		}

		/* Fill the buffers with one block from each run being merged */
		for (i=0; i < subListsInRun; i++)
		{
			fseek(file, runOffset[i], SEEK_SET);
			metric->num_reads += 1;
			if (0 == fread(&buffer[i * es->page_size], es->page_size, 1, file))		
				return 10;
			
			#if defined(DEBUG)
				addr = &(buffer[i * es->page_size]);
				printf("  FIRST MERGE Offset: %d # blocks: %d Block header: %d  Records: %d  First record: %p  Record key: %d\n",runOffset[i],runCount[i],*((int32_t*) addr), *((int16_t*) (addr+4)), (addr+6), ((test_record_t*) (addr+6))->key);
			#endif
		}

		/* Continually find lowest tuple in the run and write to output buffer */
		numblocks = 0;
		bufferOutputPos = es->headerSize;  /* points to next empty tuple position in buffer block */ // Start after header - not at 0	
		while (1)
		{					
			/* Find smallest record */
			i = 0;
			while (runCount[i] == 0 && i < subListsInRun)
				i++;
			if (i == subListsInRun)
				break;					/* Processed all input */
			lowId = i;			
			tuple = (test_record_t*) (buffer + es->headerSize  + i * es->page_size + sublsTuplePos[i] * es->record_size);
			i++;
			for ( ; i < subListsInRun; i++)
			{
				if (0 == runCount[i])				
					continue; 			/* Run has been completely used */

				value = (test_record_t*) (buffer + es->headerSize  + i * es->page_size + sublsTuplePos[i] * es->record_size);
				metric->num_compar++;

				if (0 < compareFn(tuple, value))
				{
					lowId = i;
					tuple = value;
				}
			}			
				
			/* Add tuple to buffer */
			metric->num_memcpys++;			
			memcpy((buffer + (bufferSizeInBlocks - 1) * es->page_size + bufferOutputPos), (void*) tuple, es->record_size);
			bufferOutputPos += es->record_size;

			/* if the buffer is full write it out */
			if (bufferOutputPos >= es->page_size - es->record_size)
			{
				fseek(file, lastWritePos, SEEK_SET);
				/* Output the block */
				*((int32_t*) &buffer[(bufferSizeInBlocks - 1) * es->page_size]) = numblocks++;							/* Block index */
				*((int16_t*) (&buffer[(bufferSizeInBlocks - 1) * es->page_size]+4)) = bufferOutputPos/es->record_size;	/* Block record count */
				if (0 == fwrite(&buffer[(bufferSizeInBlocks - 1) * es->page_size], es->page_size, 1, file))							 
					return 9;
				
				/* Used to check output buffer is correct when writing */
				addr = &buffer[(bufferSizeInBlocks - 1) * es->page_size];
				#if defined(DEBUG)
					printf("OUTPUT Block Offset: %d Block header: %d  Records: %d  First record: %p  Record key: %d\n",last_writePos,*((int32_t*) addr), *((int16_t*) (addr+4)), (addr+6), ((test_record_t*) (addr+6))->key);
				#endif
				#if defined(DEBUG)
				/* 
					for (int a=0; a < *((int16_t*) (addr+4)); a++)
					{	test_record_t* tmptuple = addr+a*es->record_size+6;
						printf("Key: %d  Address: %d\n", tmptuple->key, tmptuple);
					}
				*/
				#endif	
				lastWritePos = ftell(file); 
				bufferOutputPos = es->headerSize;
				metric->num_writes += 1;
			}
			
			/* Increment to next tuple of block */
			sublsTuplePos[lowId]++;

			/* Check if have more tuples */
			addr = &(buffer[lowId * es->page_size]);
			if (sublsTuplePos[lowId] >= *((int16_t*) (addr+4)))
			{
				/* Increment to next block */
				runOffset[lowId] += es->page_size;
				runCount[lowId]--;
				sublsTuplePos[lowId] = 0;

				/* Check if we are finished with that sublist */
				if (runCount[lowId] > 0)
				{
					/* Read in next block */
					fseek(file, runOffset[lowId], SEEK_SET);

					if (0 == fread(&buffer[lowId * es->page_size], es->page_size, 1, file))					
						return 10;
					
					metric->num_reads += 1;					
				}
			}			
		}

		/* Write out output buffer if partially full */
		if (bufferOutputPos > es->headerSize)
		{
			fseek(file, lastWritePos, SEEK_SET);
			/* Output the block */
			*((int32_t*) &buffer[(bufferSizeInBlocks - 1) * es->page_size]) = numblocks++;							/* Block index */
			*((int16_t*) (&buffer[(bufferSizeInBlocks - 1) * es->page_size]+4)) = bufferOutputPos/es->record_size;	/* Block record count */
			if (0 == fwrite(&buffer[(bufferSizeInBlocks - 1) * es->page_size], es->page_size, 1, file))							 
				return 9;
			
			/* Used to check output buffer is correct when writing */
			addr = &buffer[(bufferSizeInBlocks - 1) * es->page_size];
			#if defined(DEBUG)
				printf("OUTPUT (partial) Block Offset: %d Block header: %d  Records: %d  First record: %p  Record key: %d\n",last_writePos,*((int32_t*) addr), *((int16_t*) (addr+4)), (addr+6), ((test_record_t*) (addr+6))->key);
			#endif					
						
			lastWritePos = ftell(file); 
			bufferOutputPos = es->headerSize;
			metric->num_writes += 1;
		}		
		numSublist = numSublist - subListsInRun + 1;
	} /* End of merge */

	/* Return pointer to sorted output */
	*resultFilePtr = ptrNextFirst;

	// Cleanup
	free(sublsTuplePos);
	free(runOffset);
	free(runCount);
	return 0;
}
