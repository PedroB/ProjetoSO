#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <string.h>
#include <pthread.h>

#include "constants.h"
#include "parser.h"
#include "operations.h"

// Structure to hold arguments for thread function
typedef struct {
    char in_path[MAX_JOB_FILE_NAME_SIZE];
    char out_path[MAX_JOB_FILE_NAME_SIZE];
    // DIR *dir[MAX_JOB_FILE_NAME_SIZE];
   DIR *dir;

    char dir_name[MAX_JOB_FILE_NAME_SIZE];
    
  
} ThreadArg;

int backup_counter_files = 0;
int simultaneous_backups = 0;

  void readFilesLines(int fd_in, int fd_out, char* dir_name, struct dirent* entry, char *in_path, char *out_path){ 

  while (1) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;
    char buffer[MAX_PIPE];


    fflush(stdout);
    switch (get_next(fd_in)) {
      case CMD_WRITE:
        num_pairs = parse_write(fd_in, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_write(num_pairs, keys, values)) {
          fprintf(stderr, "Failed to write pair\n");
        }

        break;

      case CMD_READ:

        num_pairs = parse_read_delete(fd_in, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_read(num_pairs, keys, fd_out)) {
          fprintf(stderr, "Failed to read pair\n");
        }
        break;

      case CMD_DELETE:

        num_pairs = parse_read_delete(fd_in, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_delete(num_pairs, keys, fd_out)) {
          fprintf(stderr, "Failed to delete pair\n");
        }
        break;

      case CMD_SHOW:

        kvs_show(fd_out);
        break;

      case CMD_WAIT:
      puts("wait");

        if (parse_wait(fd_in, &delay, NULL) == -1) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (delay > 0) {

          sprintf(buffer, "%s\n", "Waiting...\n");
          mywrite(fd_out, buffer);

          kvs_wait(delay);
        }
        break;

      case CMD_BACKUP:

        if (kvs_backup(dir_name, entry, in_path, out_path)) {
          fprintf(stderr, "Failed to perform backup.\n");
        }
        break;

      case CMD_INVALID:

        fprintf(stderr, "Invalid command. See HELP for usage\n");
        break;

      case CMD_HELP:
      
        sprintf(buffer,  "Available commands:\n"
            "  WRITE [(key,value)(key2,value2),...]\n"
            "  READ [key,key2,...]\n"
            "  DELETE [key,key2,...]\n"
            "  SHOW\n"
            "  WAIT <delay_ms>\n"
            "  BACKUP\n" // Not implemented
            "  HELP\n");
        mywrite(fd_out, buffer);


        break;
      
      case EOC:
        return;
      case CMD_EMPTY:
      default:
        break;    
  }
  }
  }

int gen_path(char* dir_name, struct dirent* entry, char *in_path, char *out_path) {
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

    // Append '/' and entry->d_name
    strncat(in_path, "/", MAX_JOB_FILE_NAME_SIZE - strlen(in_path) - 1);
    strncat(in_path, entry->d_name, MAX_JOB_FILE_NAME_SIZE - strlen(in_path) - 1);

  char *ptr_to_dot = strrchr(entry->d_name, '.');
    if (ptr_to_dot && strcmp(ptr_to_dot, ".job") == 0) {
        // Create output file path
        snprintf(out_path, MAX_JOB_FILE_NAME_SIZE, "%s/%.*s.out", 
                 dir_name, (int)(ptr_to_dot - entry->d_name), entry->d_name);

        // Call readFilesLines and redirect output to .out file
        int out_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (out_fd == -1) {
            perror("Failed to open output file");
            return 1;
        }
        return 0;
    }
    return 1;

}

void *processFiles(void *arg) {
    ThreadArg *threadArg = (ThreadArg *)arg; // Cast to the correct type
    struct dirent *entry;
    DIR *dir = threadArg->dir;
    char *dir_name = threadArg->dir_name;
    char in_path[MAX_JOB_FILE_NAME_SIZE], out_path[MAX_JOB_FILE_NAME_SIZE];

    while ((entry = readdir(dir)) != NULL) {
        if (gen_path(dir_name, entry, in_path, out_path)) {
            continue;
        }

        int fd_in = open(in_path, O_RDONLY);
        int fd_out = open(out_path, O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR);

        readFilesLines(fd_in, fd_out, dir_name, entry, in_path, out_path);
    }

    return NULL; // Return NULL to satisfy pthread_create requirements
}


int main(int argc,char *argv[]) {
  
  char* dir_name = argv[1];
  DIR *dir;
  dir = opendir(dir_name);
  

  if (argc != 2) {
        fprintf(stderr, "Usage: %s <directory>\n", argv[0]);
        return 1;
    }

  if(dir == NULL){
    fprintf(stderr, "Cannot open filename '%s'\n",dir_name);

    return 1;
  }


  if (kvs_init()) {
    fprintf(stderr, "Failed to initialize KVS\n");
    return 1;
  }

    pthread_t threads[MAX_THREADS];
    int thread_count = 0;

      

  char in_path[MAX_JOB_FILE_NAME_SIZE],out_path[MAX_JOB_FILE_NAME_SIZE];
  for(int i = 0; i < MAX_THREADS;i++){
      if(thread_count >= MAX_THREADS){
      break;
    }
  
    // Create argument for thread
        ThreadArg* arg = (ThreadArg*)malloc(sizeof(ThreadArg));
        strcpy(arg->in_path, in_path);
        strcpy(arg->out_path, out_path);
        strcpy(arg->dir_name, dir_name);
        arg->dir=dir;

        // Create thread to process the file
        if (pthread_create(&threads[thread_count], NULL, processFiles, (void *)arg) != 0) {
        fprintf(stderr, "Error creating thread\n");
        free(arg); // Free argument if thread creation fails
        continue;
    }
        thread_count++;
}


  // Join all successfully created threads
  for (int i = 0; i < thread_count; i++) {
      if (pthread_join(threads[i], NULL) != 0) {
          fprintf(stderr, "Error joining thread %d\n", i);
      }
  }
  closedir(dir);
  kvs_terminate();

  return 0;
}

