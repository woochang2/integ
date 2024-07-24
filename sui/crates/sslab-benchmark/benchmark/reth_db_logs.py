
from collections import defaultdict


class BlockInsertionMetrics:
    
    PATTERN = r'Inserted block block_number=\d+ actions=\[\(InsertCanonicalHeaders, (\d+(?:\.\d+)?)([mnµs]+)\), \(InsertHeaders, (\d+(?:\.\d+)?)([mnµs]+)\), \(InsertHeaderNumbers, (\d+(?:\.\d+)?)([mnµs]+)\), \(GetParentTD, (\d+(?:\.\d+)?)([mnµs]+)\), \(InsertHeaderTD, (\d+(?:\.\d+)?)([mnµs]+)\), \(GetNextTxNum, (\d+(?:\.\d+)?)([mnµs]+)\), \(InsertTxSenders, (\d+(?:\.\d+)?)([mnµs]+)\), \(InsertTransactions, (\d+(?:\.\d+)?)([mnµs]+)\), \(InsertTxHashNumbers, (\d+(?:\.\d+)?)([mnµs]+)\), \(InsertBlockBodyIndices, (\d+(?:\.\d+)?)([mnµs]+)\), \(InsertTransactionBlock, (\d+(?:\.\d+)?)([mnµs]+)\)\]'
    
    def __init__(self):
        self.n = 0
        
        self.canonical_headers = 0.0
        self.headers = 0.0
        self.header_numbers = 0.0
        self.get_parent_td = 0.0
        self.header_td = 0.0
        self.get_next_tx_num = 0.0
        self.tx_senders = 0.0
        self.transactions = 0.0
        self.tx_hash_numbers = 0.0
        self.block_body_indices = 0.0
        self.transaction_block = 0.0
        
    def update(self, *args):
        assert len(args) == 11
        self.n += 1
        self.canonical_headers += args[0]
        self.headers += args[1]
        self.header_numbers += args[2]
        self.get_parent_td += args[3]
        self.header_td += args[4]
        self.get_next_tx_num += args[5]
        self.tx_senders += args[6]
        self.transactions += args[7]
        self.tx_hash_numbers += args[8]
        self.block_body_indices += args[9]
        self.transaction_block += args[10]
        
    def add(self, other):
        assert isinstance(other, BlockInsertionMetrics)
        
        self.n += other.n
        self.canonical_headers += other.canonical_headers
        self.headers += other.headers
        self.header_numbers += other.header_numbers
        self.get_parent_td += other.get_parent_td
        self.header_td += other.header_td
        self.get_next_tx_num += other.get_next_tx_num
        self.tx_senders += other.tx_senders
        self.transactions += other.transactions
        self.tx_hash_numbers += other.tx_hash_numbers
        self.block_body_indices += other.block_body_indices
        self.transaction_block += other.transaction_block
        
    def extend(self, others):
        for other in others:
            self.add(other)
        return self
        
    def report(self) -> str:
        return (
            f' \t      - CanonicalHeaders: {self.canonical_headers / self.n:.2f} µs\n'
            f' \t      - Headers: {self.headers / self.n:.2f} µs\n'
            f' \t      - HeaderNumbers: {self.header_numbers / self.n:.2f} µs\n'
            f' \t      - GetParentTd: {self.get_parent_td / self.n:.2f} µs\n'
            f' \t      - HeaderTd: {self.header_td / self.n:.2f} µs\n'
            f' \t      - GetNextTxNum: {self.get_next_tx_num / self.n:.2f} µs\n'
            f' \t      - TxSenders: {self.tx_senders / self.n:.2f} µs\n'
            f' \t      - Transactions: {self.transactions / self.n:.2f} µs\n'
            f' \t      - TxHashNumbers: {self.tx_hash_numbers / self.n:.2f} µs\n'
            f' \t      - BlockBodyIndices: {self.block_body_indices / self.n:.2f} µs\n'
            f' \t      - TransactionBlock: {self.transaction_block / self.n:.2f} µs\n'
        )
        
        
class BlockAppendMetrics:
    
    PATTERN=r'Appended blocks range=\d+..=\d+ actions=\[\(InsertBlock, (\d+(?:\.\d+)?)([mnµs]+)\), \(InsertState, (\d+(?:\.\d+)?)([mnµs]+)\), \(InsertHashes, (\d+(?:\.\d+)?)([mnµs]+)\), \(InsertHistoryIndices, (\d+(?:\.\d+)?)([mnµs]+)\), \(UpdatePipelineStages, (\d+(?:\.\d+)?)([mnµs]+)\)\]'
    
    def __init__(self):
        self.n = 0
        
        self.insert_block = 0.0  # equals to the BlockInsertion latency
        self.insert_state = 0.0
        self.insert_hash = 0.0
        self.insert_history_indices = 0.0
        self.update_pipeline_stages = 0.0
        
    def update(self, *args):
        assert len(args) == 5
        self.n += 1
        self.insert_block += args[0]
        self.insert_state += args[1]
        self.insert_hash += args[2]
        self.insert_history_indices += args[3]
        self.update_pipeline_stages += args[4]
    
    def add(self, other):
        assert isinstance(other, BlockAppendMetrics)
        
        self.n += other.n
        self.insert_block += other.insert_block
        self.insert_state += other.insert_state
        self.insert_hash += other.insert_hash
        self.insert_history_indices += other.insert_history_indices
        self.update_pipeline_stages += other.update_pipeline_stages
        
    def extend(self, others):
        for other in others:
            self.add(other)
        return self
        
    def report_with(self, insertion: BlockInsertionMetrics) -> str:
        return (
            f' \t  - InsertBlock: {self.insert_block / self.n:.2f} µs\n'
            f'{insertion.report()}'  # BlockInsertionMetrics
            f' \t  - InsertState: {self.insert_state / self.n:.2f} µs\n'
            f' \t  - InsertHash: {self.insert_hash / self.n:.2f} µs\n'
            f' \t  - InsertHistoryIndices: {self.insert_history_indices / self.n:.2f} µs\n'
            f' \t  - UpdatePipelineStages: {self.update_pipeline_stages / self.n:.2f} µs\n'
        )
        
   
class CommitMetric:
    
    PATTERN = r'Commit total_duration=(\d+(?:\.\d+)?)([mnµs]+)'
    
    def __init__(self):
        self.n = 0
        
        self.commit = 0.0
        
    def bulk_update(self, *args):
        self.n += len(args)
        self.commit += sum(args)
        
    def add(self, other):
        assert isinstance(other, CommitMetric)
        
        self.n += other.n
        self.commit += other.commit
        
    def extend(self, others):
        for other in others:
            self.add(other)
        return self
        
    def report(self) -> str:
        return f' \t  - Commit: {self.commit / self.n:.2f} µs\n'
    

def dict_add(my_dict: defaultdict, your_dict: defaultdict):
    for key, value in your_dict.items():
        my_dict[key] += value

class CanonicalizationMetrics:
    
    PATTERN = r'Canonicalization finished block_number (\d+) actions=\[\(CloneOldBlocks, (\d+(?:\.\d+)?)([mnµs]+)\), \(FindCanonicalHeader, (\d+(?:\.\d+)?)([mnµs]+)\), \(SplitChain, (\d+(?:\.\d+)?)([mnµs]+)\), \(SplitChainForks, (\d+(?:\.\d+)?)([mnµs]+)\), \(MergeAllChains, (\d+(?:\.\d+)?)([mnµs]+)\), \(UpdateCanonicalIndex, (\d+(?:\.\d+)?)([mnµs]+)\), \(RetrieveStateTrieUpdates, (\d+(?:\.\d+)?)([mnµs]+)\), \(CommitCanonicalChainToDatabase, (\d+(?:\.\d+)?)([mnµs]+)\)\]'
    
    def __init__(self):
        self.n = defaultdict(int)
        
        self.clone_old_blocks = defaultdict(float)
        self.find_canonical_header = defaultdict(float)
        self.split_chain = defaultdict(float)
        self.split_chain_forks = defaultdict(float)
        self.merge_all_chains = defaultdict(float)
        self.update_canonical_index = defaultdict(float)
        self.retrieve_state_trie_update = defaultdict(float)
        self.commit_cononical_chain_to_database = defaultdict(float)  # includes the BlockAppend latency
        
    def update(self, block_number :int, *args):
        assert len(args) == 8
        self.n[block_number] += 1
        self.clone_old_blocks[block_number] += args[0]
        self.find_canonical_header[block_number] += args[1]
        self.split_chain[block_number] += args[2]
        self.split_chain_forks[block_number] += args[3]
        self.merge_all_chains[block_number] += args[4]
        self.update_canonical_index[block_number] += args[5]
        self.retrieve_state_trie_update[block_number] += args[6]
        self.commit_cononical_chain_to_database[block_number] += args[7]
        
    def add(self, other):
        assert isinstance(other, CanonicalizationMetrics)
        
        dict_add(self.n, other.n)
        dict_add(self.clone_old_blocks, other.clone_old_blocks)
        dict_add(self.find_canonical_header, other.find_canonical_header)
        dict_add(self.split_chain, other.split_chain)
        dict_add(self.split_chain_forks, other.split_chain_forks)
        dict_add(self.merge_all_chains, other.merge_all_chains)
        dict_add(self.update_canonical_index, other.update_canonical_index)
        dict_add(self.retrieve_state_trie_update, other.retrieve_state_trie_update)
        dict_add(self.commit_cononical_chain_to_database, other.commit_cononical_chain_to_database)
        
    def extend(self, others):
        for other in others:
            self.add(other)
        return self
        
    def report_with(self, commit: CommitMetric, append: BlockAppendMetrics, insertion: BlockInsertionMetrics) -> str:
        total_number_of_items = sum(self.n.values())
        total_latency = sum((
                sum(self.clone_old_blocks.values()),
                sum(self.find_canonical_header.values()),
                sum(self.split_chain.values()),
                sum(self.split_chain_forks.values()),
                sum(self.merge_all_chains.values()),
                sum(self.update_canonical_index.values()),
                sum(self.retrieve_state_trie_update.values()),
                sum(self.commit_cononical_chain_to_database.values()))) / total_number_of_items if total_number_of_items else 0.0
        return (
            f' CanonicalizationMetrics: {total_latency:.2f} µs\n'
            f' \tCloneOldBlocks: {sum(self.clone_old_blocks.values()) / total_number_of_items:.2f} µs\n'
            f' \tFindCanonicalHeader: {sum(self.find_canonical_header.values()) / total_number_of_items:.2f} µs\n'
            f' \tSplitChain: {sum(self.split_chain.values()) / total_number_of_items:.2f} µs\n'
            f' \tSplitChainForks: {sum(self.split_chain_forks.values()) / total_number_of_items:.2f} µs\n'
            f' \tMergeAllChains: {sum(self.merge_all_chains.values()) / total_number_of_items:.2f} µs\n'
            f' \tUpdateCanonicalIndex: {sum(self.update_canonical_index.values()) / total_number_of_items:.2f} µs\n'
            f' \tRetrieveStateTrieUpdate: {sum(self.retrieve_state_trie_update.values()) / total_number_of_items:.2f} µs\n'
            f' \tCommitCanonicalChainToDatabase: {sum(self.commit_cononical_chain_to_database.values()) / total_number_of_items:.2f} µs\n'
            f'{append.report_with(insertion)}'
            f'{commit.report()}'
        )
        
    def report_all_according_to_block_number(self) -> str:
        result = "BlockNumber; TotalLatency; CloneOldBlocks; FindCanonicalHeader; SplitChain; SplitChainForks; MergeAllChains; UpdateCanonicalIndex; RetrieveStateTrieUpdate; CommitCanonicalChainToDatabase\n"
        for block_number, total_number_of_items in sorted(self.n.items()):
            total_latency = sum((
                    self.clone_old_blocks[block_number],
                    self.find_canonical_header[block_number],
                    self.split_chain[block_number],
                    self.split_chain_forks[block_number],
                    self.merge_all_chains[block_number],
                    self.update_canonical_index[block_number],
                    self.retrieve_state_trie_update[block_number],
                    self.commit_cononical_chain_to_database[block_number])) / total_number_of_items if total_number_of_items else 0.0
            result += (
                f'{block_number} {total_latency:.2f} {self.clone_old_blocks[block_number] / total_number_of_items:.2f} {self.find_canonical_header[block_number] / total_number_of_items:.2f}'
                f' {self.split_chain[block_number] / total_number_of_items:.2f} {self.split_chain_forks[block_number] / total_number_of_items:.2f} {self.merge_all_chains[block_number] / total_number_of_items:.2f}'
                f' {self.update_canonical_index[block_number] / total_number_of_items:.2f} {self.retrieve_state_trie_update[block_number] / total_number_of_items:.2f} {self.commit_cononical_chain_to_database[block_number] / total_number_of_items:.2f}\n'
            )
        return result
            


def convert_to_micros(duration: float, unit: str) -> float:
    if unit == 's':
        return duration * 1_000_000
    elif unit == 'ms':
        return duration * 1_000
    elif unit == 'µs':
        return duration
    elif unit == 'ns':
        return duration / 1_000
    else:
        raise ValueError('Invalid unit')
    
    
def pairwise(it):
    it = iter(it)
    while True:
        try:
            yield next(it), next(it)
        except StopIteration:
            # no more elements in the iterator
            return
