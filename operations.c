#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <time.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/wait.h>
#include <stdio.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>

#include "kvs.h"
#include "constants.h"


static struct HashTable* kvs_table = NULL;

// file2.c
extern int simultaneous_backups; 


typedef struct {
    char job_file_name[MAX_JOB_FILE_NAME_SIZE];
    int backup_count;
} JobBackup;

// typedef struct{
//   char key[MAX_JOB_FILE_NAME_SIZE];
//   char value[MAX_JOB_FILE_NAME_SIZE];
//   pthread_rwlock_t lock;
  
// }Node;




JobBackup backup_tracker[MAX_JOB_FILES];  // Array to store job files and their counts
int backup_tracker_size = 0;   // number of job files



/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}


///
int mywrite(int fd, char *buffer) {

    /* because we don't want to write the ending null-character to the file
     * ~~~~~~~~~~~~~~~~~~~~~~vvv */
    int len =  (int) strlen(buffer);
    int done = 0;

    while (len > done) {
        int bytes_written = (int) write(fd, buffer + done, (size_t) (len - done));

        if (bytes_written < 0) {
            perror("write error");
            return -1;
        }

        /*
         * it might not have managed to write all data.
         * if you're curious, try to find out why, in this case, the program
         * will always be able to write it all.
         */
        done += bytes_written;
    }
    return done;
}





int kvs_init() {
  if (kvs_table != NULL) {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();
  return kvs_table == NULL;
}

int kvs_terminate() {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  //destroy all the locks
  for(int i = 0; i < TABLE_SIZE; i++) {
      pthread_rwlock_destroy(&kvs_table->locks[i]);
    }

  free_table(kvs_table);
  return 0;
}


void order_keys(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
  size_t swapped;
  do {
    swapped = 0;
    for (size_t i = 0; i < num_pairs - 1; i++) {
      if (strcmp(keys[i], keys[i + 1]) > 0) {
        // Swap keys
        char temp_key[MAX_STRING_SIZE];
        strcpy(temp_key, keys[i]);
        strcpy(keys[i], keys[i + 1]);
        strcpy(keys[i + 1], temp_key);

        // Swap corresponding values
        char temp_value[MAX_STRING_SIZE];
        strcpy(temp_value, values[i]);
        strcpy(values[i], values[i + 1]);
        strcpy(values[i + 1], temp_value);

        swapped = 1;
      }
    }
  } while (swapped);
}



// char **order_keys(size_t num_pairs, char **keys, char **values) {
//   size_t swapped;
//   do {
//     swapped = 0;
//     for (size_t i = 0; i < num_pairs - 1; i++) {
//       if (strcmp(keys[i], keys[i + 1]) > 0) {
//         // Swap keys
//         char *temp_key = keys[i];
//         keys[i] = keys[i + 1];
//         keys[i + 1] = temp_key;

//         // Swap corresponding values
//         char *temp_value = values[i];
//         values[i] = values[i + 1];
//         values[i + 1] = temp_value;

//         swapped = 1;
//       }
//     }
//   } while (swapped);

//   return keys; // Return sorted keys (optional)
// }


// void unlock_keys(size_t num_pairs, char keys[][MAX_STRING_SIZE]) {
//   for (int i = 0; i < TABLE_SIZE; i++) {
//     KeyNode *keyNode = kvs_table->table[i];
//     while (keyNode != NULL) {
//       for (size_t j = 0; j < num_pairs; j++) {
//         if (strcmp(keyNode->key, keys[j]) == 0) { // Compare strings correctly
//         }
//       }
//       keyNode = keyNode->next; // Move to the next node
//     }
//   }
// }

void lock_keys(size_t num_pairs, char keys[][MAX_STRING_SIZE], int mode) {

  int index;
  bool boolean_locks[TABLE_SIZE] = {false}; // All elements will be initialized to false
  
  //filter duplicates while filling the boolean_locks
  for(size_t i = 0; i < num_pairs; i++) {

    index = hash(keys[i]);
    if (boolean_locks[index] == false) {
      boolean_locks[index] = true;
    }
  }

  for(int i = 0; i < TABLE_SIZE; i++) {

    if (boolean_locks[i] == true) {
      if(mode == READ) {
        pthread_rwlock_rdlock(&kvs_table->locks[i]);

      } else if(mode == WRITE) {
        pthread_rwlock_wrlock(&kvs_table->locks[i]);
      }
    }
  }

}

void unlock_keys(size_t num_pairs, char keys[][MAX_STRING_SIZE]) {

  int index;
  bool boolean_locks[TABLE_SIZE] = {false}; // All elements will be initialized to false
  
  //filter duplicates while filling the boolean_locks
  for(size_t i = 0; i < num_pairs; i++) {

    index = hash(keys[i]);
    if (boolean_locks[index] == false) {
      boolean_locks[index] = true;
    }
  }

  for(int i = 0; i < TABLE_SIZE; i++) {

    if (boolean_locks[i] == true) {
      pthread_rwlock_unlock(&kvs_table->locks[i]);
    }
  }

}

// void unlock_keys(size_t num_pairs, char **keys) {
//   for (int i = 0; i < TABLE_SIZE; i++) {
//     KeyNode *keyNode = kvs_table->table[i];
//     while (keyNode != NULL) {
//       for (size_t j = 0; j < num_pairs; j++) {
//         if (strcmp(keyNode->key, keys[j]) == 0) { // Compare strings correctly
//           pthread_rwlock_unlock(&keyNode->lock); // Unlock the correct lock
//         }
//       }
//       keyNode = keyNode->next; // Move to the next node
//     }
//   }
// }


int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  order_keys(num_pairs,keys,values);
  lock_keys(num_pairs, keys, WRITE);

  for (size_t i = 0; i < num_pairs; i++) {


    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
    }
  }

  unlock_keys(num_pairs,keys);
  

  return 0;
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  char values[MAX_STRING_SIZE][MAX_STRING_SIZE] = {};

  order_keys(num_pairs,keys,values);
  lock_keys(num_pairs, keys, READ);

  
  char buffer[MAX_PIPE];                          

  
  sprintf(buffer,"[");     
  mywrite(fd, buffer);
  for (size_t i = 0; i < num_pairs; i++) {
    char* result = read_pair(kvs_table, keys[i]);
    if (result == NULL) {
      // printf("(%s,KVSERROR)", keys[i]);

      sprintf(buffer, "(%s,KVSERROR)", keys[i]);     
      mywrite(fd, buffer);
    } else {
      // printf("(%s,%s)", keys[i], result);
            // mywrite(fd,"(%s,%s)", keys[i], result);
     sprintf(buffer,"(%s,%s)", keys[i], result);     
      mywrite(fd, buffer);

    }
    free(result);
  }
  // printf("]\n");
    //  mywrite(fd,"]\n");
  sprintf(buffer,"]\n");     
  mywrite(fd, buffer);


 unlock_keys(num_pairs,keys);

  return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  // char *values = NULL;
  // order_keys(num_pairs,keys,values);

  char values[MAX_STRING_SIZE][MAX_STRING_SIZE] = {};
  order_keys(num_pairs, keys, values);

  lock_keys(num_pairs, keys, WRITE);

  int aux = 0;

  char buffer[MAX_PIPE];

  for (size_t i = 0; i < num_pairs; i++) {
    if (delete_pair(kvs_table, keys[i]) != 0) {
      if (!aux) {
        sprintf(buffer,"[");     
        mywrite(fd, buffer);

        // printf("[");
        aux = 1;
      }
      sprintf(buffer,"(%s,KVSMISSING)", keys[i]);     
      mywrite(fd, buffer);
      // printf("(%s,KVSMISSING)", keys[i]);
    }
  }
  if (aux) {
    // printf("]\n");
    sprintf(buffer,"]\n");     
    mywrite(fd, buffer);

  }

  unlock_keys(num_pairs,keys);

  return 0;
}

void kvs_show(int fd) {


  // lock the whole table
  for(int i = 0; i < TABLE_SIZE; i++) {
      pthread_rwlock_rdlock(&kvs_table->locks[i]);
    }


  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i];
  
    while (keyNode != NULL) {

      char buffer[MAX_PIPE];                          
      sprintf(buffer,  "(%s, %s)\n", keyNode->key, keyNode->value);          
      write(fd, buffer, strlen(buffer));

      keyNode = keyNode->next; // Move to the next node
    }
  }

  // for (int i = 0; i < TABLE_SIZE; i++) {
  //   KeyNode *keyNode = kvs_table->table[i];
  //   while (keyNode != NULL) {
        

  //   }
  //     keyNode = keyNode->next; // Move to the next node
  //   }

  // unlock the whole table
  for(int i = 0; i < TABLE_SIZE; i++) {
      pthread_rwlock_unlock(&kvs_table->locks[i]);
    }

  }


/// Waits for the last backup to be called.
void kvs_wait_backup() {
    wait(NULL); // Wait for any child process to terminate
    simultaneous_backups --; // Decrement the active backup counter
}




int get_backup_count(const char *job_file_name) {
    // printf("backup tracker size: %d\n", backup_tracker_size);
    for (int i = 0; i < MAX_JOB_FILES; i++) {
        // printf("job file name na struc jobbackup: %s e num_backup: %d \n", backup_tracker[i].job_file_name,backup_tracker[backup_tracker_size].backup_count);

        if (strcmp(backup_tracker[i].job_file_name, job_file_name) == 0) {
            backup_tracker[i].backup_count++;
          
            return backup_tracker[i].backup_count;
        }
    }
  

    // If not found, add a new entry
    if (backup_tracker_size < MAX_JOB_FILES) {
        strncpy(backup_tracker[backup_tracker_size].job_file_name, job_file_name, MAX_JOB_FILE_NAME_SIZE - 1);
        backup_tracker[backup_tracker_size].job_file_name[MAX_JOB_FILE_NAME_SIZE - 1] = '\0'; // Null-terminate
        backup_tracker[backup_tracker_size].backup_count = 1;

        
        
        backup_tracker_size ++;

        return backup_tracker[backup_tracker_size - 1].backup_count;
    }

    fprintf(stderr, "Error: Backup tracker full.\n");
    return -1;
}


int gen_path_backup(char* dir_name, struct dirent* entry, char *in_path, char *out_path) {

    if (!dir_name || !entry || !in_path || !out_path) {
        return 1; 
    }
    // Check the total length of dir_name and entry->d_name
    size_t dir_len = strlen(dir_name);
    size_t file_len = strlen(entry->d_name);

    // Ensure the combined path fits within MAX_JOB_FILE_NAME_SIZE
    if (dir_len + 1 + file_len + 1 > MAX_JOB_FILE_NAME_SIZE) {
        fprintf(stderr, "Error: Combined path length exceeds MAX_JOB_FILE_NAME_SIZE.\n");
        return 1;
    }

    // Copy directory name to in_path
    strncpy(in_path, dir_name, MAX_JOB_FILE_NAME_SIZE - 1);
    in_path[MAX_JOB_FILE_NAME_SIZE - 1] = '\0';  // Ensure null termination

    strncat(in_path, "/", MAX_JOB_FILE_NAME_SIZE - strlen(in_path) - 1);
    strncat(in_path, entry->d_name, MAX_JOB_FILE_NAME_SIZE - strlen(in_path) - 1);

  char *ptr_to_dot = strrchr(entry->d_name, '.');
    if (ptr_to_dot && strcmp(ptr_to_dot, ".job") == 0) {

        int backup_count = get_backup_count(entry->d_name);

        int pid = fork();
        simultaneous_backups --; 

        if (pid == 0) {

          if (backup_count == -1) {
             exit(1);  
          }

          snprintf(out_path, MAX_JOB_FILE_NAME_SIZE, "%s/%.*s-%d.bck",
                  dir_name, (int)(ptr_to_dot - entry->d_name), entry->d_name, backup_count); 
          
          // Call readFilesLines and redirect output to .out file
          int out_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
          
          if (out_fd == -1) {
              perror("Failed to open output file");
              exit(1);
          }

        kvs_show(out_fd);
        exit(0); 

      } else if (pid > 0) {
        simultaneous_backups++;
      }
    }
        return 0;
}

int kvs_backup(char* dir_name, struct dirent* entry, char *in_path, char *out_path) {

    if(simultaneous_backups >= MAX_BACKUP) {
      kvs_wait_backup();
    }


    gen_path_backup(dir_name, entry, in_path, out_path);

    

  return 0;
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}