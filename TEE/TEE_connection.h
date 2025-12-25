//
// Created by Yufei on 2024/9/5.
//

#ifndef TEE_CONNECTION_H
#define TEE_CONNECTION_H

#ifdef WITH_TEE 

#include <err.h>
#include <string.h>
#include <iostream>

/* OP-TEE TEE client API (built by optee_client) */
#include <tee_client_api.h>

/* For the UUID (found in the TA's h-file(s)) */
#include "tee_connection_ta.h"


class TEE_connection {
  public:
    TEE_connection(int32_t id): id(id) {

     /* Initialize a context connecting us to the TEE */
     res = TEEC_InitializeContext(NULL, &ctx);
     if (res != TEEC_SUCCESS)
      errx(1, "TEEC_InitializeContext failed with code 0x%x", res);

     /*
      * Open a session to the "hello world" TA, the TA will print "hello
      * world!" in the log when the session is created.
      */
     res = TEEC_OpenSession(&ctx, &sess, &uuid,
                    TEEC_LOGIN_PUBLIC, NULL, NULL, &err_origin);
     if (res != TEEC_SUCCESS)
      errx(1, "TEEC_Opensession failed with code 0x%x origin 0x%x",
          res, err_origin);
    }
    ~TEE_connection() {
      /*
       * We're done with the TA, close the session and
       * destroy the context.
       *
       * The TA will print "Goodbye!" in the log when the
       * session is closed.
       */
     closeConnection();
    }

    void closeConnection(){
      TEEC_CloseSession(&sess);
      TEEC_FinalizeContext(&ctx);
    }

    bool is_equal(int first, int second){
      /*
       * Execute a function in the TA by invoking it, in this case
       * we're incrementing a number.
       *
       * The value of command ID part and how the parameters are
       * interpreted is part of the interface provided by the TA.
       */
      /* Clear the TEEC_Operation struct */
      memset(&op, 0, sizeof(op));
      /*
       * Prepare the argument. Pass a value in the first parameter,
       * the remaining three parameters are unused.
       */
      op.paramTypes = TEEC_PARAM_TYPES(TEEC_VALUE_INOUT, TEEC_VALUE_INOUT,
                                       TEEC_VALUE_INOUT, TEEC_NONE);
      op.params[0].value.a = first;
      op.params[1].value.a = second;
      op.params[2].value.a = 0;
      /*
       * TA_HELLO_WORLD_CMD_INC_VALUE is the actual function in the TA to be
       * called.
       */
      std::cout << "Invoking TA to compare" << op.params[0].value.a << " " << op.params[1].value.a << std::endl;
      res = TEEC_InvokeCommand(&sess, TA_TEE_CONNECTION_COMPAIRE, &op,
                   &err_origin);
      if (res != TEEC_SUCCESS)
          errx(1, "TEEC_InvokeCommand failed with code 0x%x origin 0x%x",
              res, err_origin);
      std::cout << "TA results: " << op.params[2].value.a << std::endl;
      return op.params[2].value.a;
    }

    int decode(int ciphertext, int offset, int mod){
          /*
           * Execute a function in the TA by invoking it, in this case
           * we're incrementing a number.
           *
           * The value of command ID part and how the parameters are
           * interpreted is part of the interface provided by the TA.
           */

          /* Clear the TEEC_Operation struct */
          memset(&op, 0, sizeof(op));

          /*
           * Prepare the argument. Pass a value in the first parameter,
           * the remaining three parameters are unused.
           */
          op.paramTypes = TEEC_PARAM_TYPES(TEEC_VALUE_INOUT, TEEC_VALUE_INOUT,
                                           TEEC_NONE, TEEC_NONE);
          op.params[0].value.a = ciphertext;
          op.params[1].value.a = offset;
          op.params[1].value.b = mod;
          /*
           * TA_HELLO_WORLD_CMD_INC_VALUE is the actual function in the TA to be
           * called.
           */
          std::cout << "Invoking TA to decrypt " << op.params[0].value.a << ", offset " << op.params[1].value.a << ", mod " << op.params[1].value.b << std::endl;
          res = TEEC_InvokeCommand(&sess, TA_TEE_CONNECTION_DECODE_ID, &op,
                       &err_origin);
          if (res != TEEC_SUCCESS)
              errx(1, "TEEC_InvokeCommand failed with code 0x%x origin 0x%x",
                  res, err_origin);
          std::cout << "TA results: " << op.params[0].value.a << std::endl;
          return op.params[0].value.a;
      }

    double min(double a, double b){
      shm_0.size = sizeof(double) * 2;
      shm_0.flags = TEEC_MEM_INPUT | TEEC_MEM_OUTPUT;
      res = TEEC_AllocateSharedMemory(&ctx, &shm_0);
      if (res != TEEC_SUCCESS) {
        printf("TEEC_AllocateSharedMemory failed: 0x%x\n", res);
      }
      double * shared_data = (double *) shm_0.buffer;
      shared_data[0] = a;
      shared_data[1] = b;

      memset(&op, 0, sizeof(op));
      op.paramTypes = TEEC_PARAM_TYPES(TEEC_MEMREF_WHOLE, TEEC_NONE, TEEC_NONE, TEEC_NONE);
      op.params[0].memref.parent = &shm_0;

      res = TEEC_InvokeCommand(&sess, TA_TEE_CONNECTION_SHARED_MEM, &op, &err_origin);
      if (res != TEEC_SUCCESS) {
          printf("TEEC_InvokeCommand failed: 0x%x, origin: 0x%x\n", res, err_origin);
      } else {
          printf("Response from TA: %f\n", shared_data[0]);
      }
      return shared_data[0];

    }

    void sssp_compare(){
      memset(&op, 0, sizeof(op));
      op.paramTypes = TEEC_PARAM_TYPES(TEEC_MEMREF_WHOLE, TEEC_MEMREF_WHOLE, TEEC_NONE, TEEC_NONE);
      op.params[0].memref.parent = &shm_0;
      op.params[1].memref.parent = &shm_1;



      res = TEEC_InvokeCommand(&sess, TA_TEE_CONNECTION_SSSP, &op, &err_origin);
      if (res != TEEC_SUCCESS) {
          printf("TEEC_InvokeCommand failed: 0x%x, origin: 0x%x\n", res, err_origin);
      } else {
          // printf("Response from TA: %f\n", shared_data[0]);
      }
    }
    bool sssp_set(double current, double target){
      if (index >= shm_0.size)
        return false;
      auto buffer = (double *) shm_0.buffer;
      buffer[index] = current;
      buffer = (double *) shm_1.buffer;
      buffer[index] = target;
      index ++;
      return true;
    }

    TEEC_Result allocate_shared_memory_sssp(){
      size_t size = 1024 * 64 * sizeof(double);
      shm_0.size = size;
      shm_0.flags = TEEC_MEM_INPUT | TEEC_MEM_OUTPUT;
      res = TEEC_AllocateSharedMemory(&ctx, &shm_0);
      if (res != TEEC_SUCCESS) {
        printf("TEEC_AllocateSharedMemory failed: 0x%x\n", res);
      }
      shm_1.size = size;
      shm_1.flags = TEEC_MEM_INPUT | TEEC_MEM_OUTPUT;
      res = TEEC_AllocateSharedMemory(&ctx, &shm_1);
      if (res != TEEC_SUCCESS) {
        printf("TEEC_AllocateSharedMemory failed: 0x%x\n", res);
      }
      return res;
    }

    double playground(){
      shm_0.size = sizeof(double) * 8;
      shm_0.flags = TEEC_MEM_INPUT | TEEC_MEM_OUTPUT;
      res = TEEC_AllocateSharedMemory(&ctx, &shm_0);
      if (res != TEEC_SUCCESS) {
        printf("TEEC_AllocateSharedMemory failed: 0x%x\n", res);
      }
      double * shared_data = (double *) shm_0.buffer;
      shared_data[0] = 0.1;
      shared_data[1] = -4.5;

      memset(&op, 0, sizeof(op));
      op.paramTypes = TEEC_PARAM_TYPES(TEEC_MEMREF_WHOLE, TEEC_NONE, TEEC_NONE, TEEC_NONE);
      op.params[0].memref.parent = &shm_0;
      std::cout << "shm_size: " << shm_0.size << std::endl;
      std::cout << "op_size: " << op.params[0].memref.size << std::endl;


      res = TEEC_InvokeCommand(&sess, TA_TEE_CONNECTION_PLAYGROUND, &op, &err_origin);
      if (res != TEEC_SUCCESS) {
          printf("TEEC_InvokeCommand failed: 0x%x, origin: 0x%x\n", res, err_origin);
      } else {
          printf("Response from TA: %f\n", shared_data[0]);
      }

      std::cout << shared_data[0] << " " << shared_data[1] << std::endl;
      std::cout << "shm_size: " << shm_0.size << std::endl;
      std::cout << "op_size: " << op.params[0].memref.size << std::endl;

      return shared_data[0];

    }

    void* sssp_get_current_buffer(size_t &size){
      size = shm_0.size;
      return shm_0.buffer;
    }

    void* sssp_get_target_buffer(size_t &size){
      size = shm_1.size;
      return shm_1.buffer;
    }

    void resetSharedMemory(){
      index = 0;
      memset(shm_0.buffer, 0, shm_0.size);
      memset(shm_1.buffer, 0, shm_1.size);
    }

    void printBuffer(size_t len = 5){
      auto buffer = (double *) shm_0.buffer;
      std::cout << "shm_0\n";
      for (size_t i = 0; i < len; i++){
        std::cout << buffer[i] << " ";
      }
      std::cout << std::endl;

      buffer = (double *) shm_1.buffer;
      std::cout << "shm_1\n";
      for (size_t i = 0; i < len; i++){
        std::cout << buffer[i] << " ";
      }
      std::cout << std::endl;
    }

  private:
    TEEC_Result res;
    TEEC_Context ctx;
    TEEC_Session sess;
    TEEC_Operation op;
    TEEC_UUID uuid = TA_TEE_CONNECTION_UUID;
    uint32_t err_origin;

    TEEC_SharedMemory shm_0;
    TEEC_SharedMemory shm_1;
    size_t index;


    int32_t id;
};


#endif

#endif //TEE_CONNECTION_H
