#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>
#include <iostream>
#include <set>
#include "algorithm"
#include "Barrier.h"
#include "unistd.h"
#include <mutex>


struct JobContext;
struct ThreadContext;
typedef std::pair<K1*, V1*> InputPair;
typedef std::pair<K2*, V2*> IntermediatePair;
typedef std::pair<K3*, V3*> OutputPair;
void* code_for_thread(void* context);
void print_err(std::string const &err_msg);
void shuffle(ThreadContext* tc);

struct JobContext;

struct ThreadContext {
    std::vector<InputPair> thread_input_vec;
    std::vector<IntermediatePair> thread_inter_vec;
    std::vector <std::vector <IntermediatePair> > thread_vec_to_reduce;
    std::vector<OutputPair> thread_output_vec;
    JobContext* job_context;
    int id;
};

struct JobContext
{
    const InputVec* input_vec;
    const MapReduceClient* client;
    OutputVec* output_vec;
    int multiThreadLevel;
    std::atomic<uint64_t>* atomic_counter;
    std::atomic<unsigned long long>* atmoic_inter_counter;
    std::atomic<unsigned long long>* allocater_atomic_counter;
    std::atomic<unsigned long long>* atmoic_total_to_be_reduced;
    pthread_t* threads;
    ThreadContext* contexts;
    Barrier* barrier;
    pthread_mutex_t mtx;
    pthread_mutex_t emit3_mutex;
    pthread_mutex_t mtx_for_getJob;
    pthread_mutex_t mtx_for_waitJob;
    std::vector <std::vector <IntermediatePair> > general_vec;
    std::atomic <bool> wait_was_called;
    std::atomic <bool> is_reduce_counter_inited;
    std::atomic <bool> is_map_counter_inited;
};

bool compare_k2(const IntermediatePair& inp1, const IntermediatePair& inp2)
{
    return *(inp1.first) < *(inp2.first);
}

JobHandle
startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel)
{
    //******     setting the job   *******//
    JobContext* job = new JobContext;    //todo de-allocate
    job->input_vec = &(inputVec);
    job->client = &(client);
    job->output_vec = &(outputVec);
    job->multiThreadLevel = multiThreadLevel;
    std::atomic<uint64_t>* atomic_counter = new std::atomic<uint64_t>(0);
    std::atomic<unsigned long long>* atmoic_inter_counter = new std::atomic<unsigned long long>(0);
    std::atomic<unsigned long long>* allocater_atomic_counter = new std::atomic<unsigned long long>(0);
    std::atomic<unsigned long long>* atmoic_total_to_be_reduced = new std::atomic<unsigned long long>(0);

    job->wait_was_called.store(false);
    job->is_reduce_counter_inited.store(false);
    job->is_map_counter_inited.store(false);

    job->atmoic_total_to_be_reduced = atmoic_total_to_be_reduced;
    job->atomic_counter = atomic_counter;
    job->allocater_atomic_counter = allocater_atomic_counter;
    job->atmoic_inter_counter = atmoic_inter_counter;
    pthread_t* pthread_arr = new pthread_t[multiThreadLevel];
    pthread_mutex_t mutex, emit3_mutex, mtx_for_getJob,mtx_for_waitJob;
    pthread_mutex_init(&mutex, NULL);
    pthread_mutex_init(&emit3_mutex, NULL);
    pthread_mutex_init(&mtx_for_getJob, NULL);
    pthread_mutex_init(&mtx_for_waitJob, NULL);

    job->mtx = mutex;
    job->emit3_mutex = emit3_mutex;
    job->mtx_for_getJob = mtx_for_getJob;
    job->mtx_for_waitJob = mtx_for_waitJob;
    ThreadContext* contexts_arr = new ThreadContext[multiThreadLevel];
    job->contexts = contexts_arr;
    job->threads = pthread_arr;
    Barrier* barrier = new Barrier(multiThreadLevel);
    job->barrier = barrier;
    //******     setting the job   *******//

    // creation of each thread
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        job->contexts[i].job_context = job;
    }
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        job->contexts[i].id = i;

        if(pthread_create(job->threads + i, NULL, code_for_thread, job->contexts + i) != 0)
        {
            print_err("pthread_create failed");
        }
    }

    return job;
}

bool are_vectors_empty(ThreadContext* context)
{
    for(int i = 0; i < context->job_context->multiThreadLevel; i++)
    {
        if(!context->job_context->contexts[i].thread_inter_vec.empty())
        {
            return false;
        }
    }
    return true;
}


void* code_for_thread(void* context)
{
    auto* thread_context = static_cast <ThreadContext*> (context);

    //****************** initiating atomic counter to Map Stage **********************//
    pthread_mutex_lock(&thread_context->job_context->mtx);
    if (!thread_context->job_context->is_map_counter_inited)
    {
        unsigned long long map_counter_val= (thread_context->job_context->input_vec->size());
        map_counter_val = (((unsigned long long)((map_counter_val) << 31)) + (1ULL << 62));
        thread_context->job_context->atomic_counter->store(map_counter_val);
        thread_context->job_context->is_map_counter_inited = true;
    }
    pthread_mutex_unlock(&thread_context->job_context->mtx);
    //****************** end of initiating atomic counter to MAP Stage **********************//

    auto old_value = (thread_context->job_context->allocater_atomic_counter)->fetch_add(1);
    std::vector<IntermediatePair> flag_vec;

    while (old_value < thread_context->job_context->input_vec->size())
    {
        thread_context->thread_input_vec.push_back(thread_context->job_context->input_vec->at(old_value));
        old_value = (thread_context->job_context->allocater_atomic_counter)->fetch_add(1);
        thread_context->job_context->atomic_counter->fetch_add(1);    // increment the 64-bit counter
        thread_context->job_context->client->map(thread_context->thread_input_vec.back().first,
                                                 thread_context->thread_input_vec.back().second, context);
    }
    //****************** Sort Stage *****************************//
    std::sort(thread_context->thread_inter_vec.begin(),
              thread_context->thread_inter_vec.end(), compare_k2);
    //****************** end of Sort Stage *****************************//

    //****************** Barrier *****************************//
    thread_context->job_context->barrier->barrier();
    //****************** end of Barrier *****************************//

    //****************** Shuffle Stage *****************************//
    if(thread_context->id == 0)
    {
        //****************** initiating atomic counter to shuffle Stage **********************//
        unsigned long long inter_counter_val = (thread_context->job_context->atmoic_inter_counter->load());
        inter_counter_val = (((unsigned long long)((inter_counter_val) << 31)) + (2ULL << 62));
        thread_context->job_context->atomic_counter->store(inter_counter_val);
        //****************** end of initiating atomic counter to shuffle Stage **********************//
        shuffle(thread_context);
    }
    thread_context->job_context->barrier->barrier();
    //****************** end of Shuffle Stage *****************************//

    //****************** Reduce Stage ************************************//
    //****************** initiating atomic counter to reduce Stage **********************//

    pthread_mutex_lock(&thread_context->job_context->mtx); // todo
    if (!thread_context->job_context->is_reduce_counter_inited)
    {
        unsigned long long total_to_be_reduced = (thread_context->job_context->general_vec.size());
        total_to_be_reduced = (((unsigned long long)(total_to_be_reduced) << 31) + (3ULL << 62));
        thread_context->job_context->atomic_counter->store(total_to_be_reduced);
        thread_context->job_context->is_reduce_counter_inited = true;
    }
    pthread_mutex_unlock(&thread_context->job_context->mtx); //todo

    //****************** end of initiating atomic counter to reduce Stage **********************//
    while (!(thread_context->job_context->general_vec.empty()))
    {
        pthread_mutex_lock(&thread_context->job_context->mtx);
        // working phase

        auto& vec_to_reduce = thread_context->job_context->contexts[thread_context->id].thread_vec_to_reduce;
        if (!thread_context->job_context->general_vec.empty())
        {
            auto& vec_to_push = thread_context->job_context->general_vec.back();
            vec_to_reduce.push_back(vec_to_push);
            thread_context->job_context->general_vec.pop_back();
            thread_context->job_context->atomic_counter->fetch_add(1);
            thread_context->job_context->client->reduce(&(thread_context->job_context->contexts[
                    thread_context->id].thread_vec_to_reduce)[0], context);
            thread_context->job_context->contexts[thread_context->id].thread_vec_to_reduce.pop_back();
        }

        // end of working phase
        pthread_mutex_unlock(&thread_context->job_context->mtx);
    }
    //****************** end of Reduce Stage ************************************//
    thread_context->job_context->barrier->barrier();
    return nullptr;
}

void emit2 (K2* key, V2* value, void* context)
{
    IntermediatePair my_pair = IntermediatePair(key,value);    // create the pair   todo- de-allocate
    ThreadContext* thread_context = static_cast <ThreadContext*> (context);
    thread_context->thread_inter_vec.push_back(my_pair);
    (thread_context->job_context->atmoic_inter_counter)->fetch_add(1);    // increment the atomic counter
}

void emit3 (K3* key, V3* value, void* context)
{
    ThreadContext* thread_context = static_cast <ThreadContext*> (context);
    OutputPair my_pair = OutputPair (key,value);    // create the pair   todo- de-allocate
    pthread_mutex_lock(&thread_context->job_context->emit3_mutex);
    thread_context->job_context->output_vec->push_back(my_pair);
    pthread_mutex_unlock(&thread_context->job_context->emit3_mutex);
    thread_context->thread_output_vec.push_back(my_pair);
}

void getJobState(JobHandle job, JobState* state)
{
    auto* job_context = static_cast <JobContext*>(job);
    pthread_mutex_lock(&job_context->mtx_for_getJob);
    auto atomic_counter_val = job_context->atomic_counter->load();
    if(((job_context->atomic_counter->load() >> 62) & 0x3) == UNDEFINED_STAGE)
    {
        state->stage = UNDEFINED_STAGE;
        state->percentage = 0;
    }

    else if (((atomic_counter_val >> 62) & 0x3) == MAP_STAGE)
    {
        state->stage = MAP_STAGE;
        unsigned long long overall = atomic_counter_val >> 31 & 0x7FFFFFFF;
        state->percentage = ((float(atomic_counter_val & 0x7FFFFFFF)) / float(overall)) * 100;
    }

    else if (((atomic_counter_val >> 62) & 0x3) == SHUFFLE_STAGE)
    {
        state->stage = SHUFFLE_STAGE;
        unsigned long long  overall = atomic_counter_val >> 31 & 0x7FFFFFFF;
        state->percentage = ((float(atomic_counter_val & 0x7FFFFFFF)) / float(overall)) * 100;
    }

    else if (((atomic_counter_val >> 62) & 0x3) == REDUCE_STAGE)
    {
        state->stage = REDUCE_STAGE;
        unsigned long long  overall = atomic_counter_val >> 31 & 0x7FFFFFFF;
        state->percentage = ((float(atomic_counter_val & 0x7FFFFFFF)) / float(overall)) * 100;
    }

    pthread_mutex_unlock(&job_context->mtx_for_getJob);

}

void waitForJob(JobHandle job)
{
    auto* job_context = static_cast <JobContext*>(job);
    pthread_mutex_lock(&job_context->mtx_for_waitJob);
    if (job_context->wait_was_called)
    {
        return;
    }

    job_context->wait_was_called = true;
    for (int i = 0; i < job_context->multiThreadLevel; ++i)
    {
        pthread_join(job_context->threads[i], NULL);
    }
    pthread_mutex_unlock(&job_context->mtx_for_waitJob);
}

void closeJobHandle(JobHandle job)
{
    auto* job_context = static_cast <JobContext*>(job);
    waitForJob(job_context);

    // ***************** free all the allocations ************************* //
    delete(job_context->atomic_counter);
    delete(job_context->atmoic_inter_counter);
    delete(job_context->allocater_atomic_counter);
    delete(job_context->atmoic_total_to_be_reduced);
//    for(int i = 0; i < job_context->multiThreadLevel; i++)
//    {
//        for(auto& inter_pair: job_context->contexts[i].thread_inter_vec)
//        {
//            delete (&inter_pair);
//        }
//    }
    delete[] job_context->threads;
    delete job_context->barrier;
    delete[] job_context->contexts;
    pthread_mutex_destroy(&job_context->mtx);
    pthread_mutex_destroy(&job_context->emit3_mutex);
    pthread_mutex_destroy(&job_context->mtx_for_getJob);
    delete job_context;
}

void print_err(std::string const &err_msg)
{
    std::cout << "system error: "<< err_msg << "\n";    // changed from cerr
    exit(1);
}

void shuffle(ThreadContext* tc)
{
    std::vector <std::vector <IntermediatePair> > general_vec;
    std::vector <IntermediatePair> new_vec;
    IntermediatePair curr_pair;
    IntermediatePair min;
    bool was_min_inited = false;
    int save_index = 0;

    while (!are_vectors_empty(tc))
    {
        for (int i = 0; i < tc->job_context->multiThreadLevel; i++)
        {
            if (!(tc->job_context->contexts[i].thread_inter_vec.empty()) && !was_min_inited)
            {
                min = tc->job_context->contexts[i].thread_inter_vec[0];
                was_min_inited = true;
                save_index = i;
            }

            else if (!(tc->job_context->contexts[i].thread_inter_vec.empty()))
            {
                if (*(tc->job_context->contexts[i].thread_inter_vec[0].first) < *(min.first))
                {
                    min = tc->job_context->contexts[i].thread_inter_vec[0];
                    save_index = i;
                }
            }
        }

        new_vec.push_back(min);
        tc->job_context->contexts[save_index].thread_inter_vec.erase(tc->job_context->contexts[save_index].
        thread_inter_vec.begin());
        // min holds the actual lowers K2 val

        for (int i = 0; i < tc->job_context->multiThreadLevel; i++)
        {
            while (!(tc->job_context->contexts[i].thread_inter_vec.empty()))
            {
                auto debug_key = (tc->job_context->contexts[i].thread_inter_vec[0].first);
                if(!((*debug_key) < *(min.first)) && !(*(min.first) < *debug_key))
                {
                    new_vec.push_back(tc->job_context->contexts[i].thread_inter_vec[0]);
                    tc->job_context->contexts[i].thread_inter_vec.erase(tc->job_context->contexts[i].
                    thread_inter_vec.begin());
                }
                else
                {
                    break;
                }
            }
        }
        general_vec.push_back(new_vec);
        new_vec.clear();
        was_min_inited = false;
    }
    tc->job_context->general_vec = general_vec;

}


































