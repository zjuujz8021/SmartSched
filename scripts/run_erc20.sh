#!/bin/bash

rounds=200
threads_arr=(2 4 8 12 16 20 24 28 32)
accounts_arr=(4 8 16 32 48 64 96 128 256 512 1024)
block_size_arr=(100 200 300 400 600 800 1000 2000 4000 6000 8000 10000)

threads=32
accounts=200
block_size=1000

rm -rf ./results/logs/erc20*
rm -rf ./datasets
mkdir -p ./results/logs

echo "Run benchmarks with different threads"
echo "----------------------------------------"
for thread_num in ${threads_arr[@]}; do
    ./build/bin/benchmark --type erc20 --threads $thread_num --accounts $accounts --txs.per.block $block_size --rounds $rounds --exec.stats.file ./results/logs/erc20_threads_vs_speedups.csv --all.schemes
done
echo "----------------------------------------"

echo "Run benchmarks with different accounts"
echo "----------------------------------------"
for account_num in ${accounts_arr[@]}; do
    ./build/bin/benchmark --type erc20 --threads $threads --accounts $account_num --txs.per.block $block_size --rounds $rounds --exec.stats.file ./results/logs/erc20_accounts_vs_speedups.csv --all.schemes
done
echo "----------------------------------------"

echo "Run benchmarks with different block sizes"
echo "----------------------------------------"
for block_size_num in ${block_size_arr[@]}; do
    ./build/bin/benchmark --type erc20 --threads $threads --accounts $accounts --txs.per.block $block_size_num --rounds $rounds --exec.stats.file ./results/logs/erc20_blocksize_vs_speedups.csv --all.schemes
done
echo "----------------------------------------"

# run effectiveness
echo "Run effectiveness"
echo "----------------------------------------"
threads=8
accounts_arr=(2 4 6 8 10 12 16 32 64 128)
for account_num in ${accounts_arr[@]}; do
    ./build/bin/benchmark --type erc20 --threads $threads --accounts $account_num --txs.per.block $block_size --rounds $rounds --exec.stats.file ./results/logs/erc20_effectiveness_$threads.csv --all.schemes
done

threads=32
accounts_arr=(4 8 16 32 48 64 256 1024 2048 4096)
for account_num in ${accounts_arr[@]}; do
    ./build/bin/benchmark --type erc20 --threads $threads --accounts $account_num --txs.per.block $block_size --rounds $rounds --exec.stats.file ./results/logs/erc20_effectiveness_$threads.csv --all.schemes
done
echo "----------------------------------------"
