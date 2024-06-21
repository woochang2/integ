
class BlockInsertionMetrics:

    BODY_INSERTION_LOG_PATTERN = r'Inserted block body block_number=\d+ actions=\[\(GetNextTxNum, (\d+(?:\.\d+)?)([mnµs]+)\), \(InsertTxSenders, (\d+(?:\.\d+)?)([mnµs]+)\), \(InsertTransactions, (\d+(?:\.\d+)?)([mnµs]+)\), \(InsertTxHashNumbers, (\d+(?:\.\d+)?)([mnµs]+)\), \(InsertBlockBodyIndices, (\d+(?:\.\d+)?)([mnµs]+)\), \(InsertTransactionBlock, (\d+(?:\.\d+)?)([mnµs]+)\)\]\n.*? .* storage::db::mdbx: Commit total_duration=(\d+(?:\.\d+)?)([mnµs]+)'
    HEADER_INSERTION_LOG_PATTERN = r'Inserted header block_number=\d+ actions=\[\(InsertCanonicalHeaders, (\d+(?:\.\d+)?)([mnµs]+)\), \(InsertHeaders, (\d+(?:\.\d+)?)([mnµs]+)\), \(InsertHeaderNumbers, (\d+(?:\.\d+)?)([mnµs]+)\), \(GetParentTD, (\d+(?:\.\d+)?)([mnµs]+)\), \(InsertHeaderTD, (\d+(?:\.\d+)?)([mnµs]+)\)\]'
    
    def __init__(self):
        self.n = 0
        
        #header
        self.canonical_headers = 0.0
        self.headers = 0.0
        self.header_numbers = 0.0
        self.get_parent_td = 0.0
        self.header_td = 0.0
        
        #body
        self.get_next_tx_num = 0.0
        self.tx_senders = 0.0
        self.transactions = 0.0
        self.tx_hash_numbers = 0.0
        self.block_body_indices = 0.0
        self.transaction_block = 0.0
        self.commit = 0.0
        
    def update_block_body(self, *args):
        assert len(args) == 7
        
        self.get_next_tx_num += args[0]
        self.tx_senders += args[1]
        self.transactions += args[2]
        self.tx_hash_numbers += args[3]
        self.block_body_indices += args[4]
        self.transaction_block += args[5]
        self.commit += args[6]
    
    def update_block_header(self, *args):
        assert len(args) == 5
        
        self.n += 1
        self.canonical_headers += args[0]
        self.headers += args[1]
        self.header_numbers += args[2]
        self.get_parent_td += args[3]
        self.header_td += args[4]
    
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
        self.commit += other.commit
        
    def extend(self, others):
        for other in others:
            self.add(other)
        return self
        
    def report_header(self) -> str:
        return (
            f' \t      - CanonicalHeaders: {self.canonical_headers / self.n:.2f} µs\n'
            f' \t      - Headers: {self.headers / self.n:.2f} µs\n'
            f' \t      - HeaderNumbers: {self.header_numbers / self.n:.2f} µs\n'
            f' \t      - GetParentTd: {self.get_parent_td / self.n:.2f} µs\n'
            f' \t      - HeaderTd: {self.header_td / self.n:.2f} µs\n'
            # f' \t      - GetNextTxNum: {self.get_next_tx_num / self.n:.2f} µs\n'
            # f' \t      - TxSenders: {self.tx_senders / self.n:.2f} µs\n'
            # f' \t      - Transactions: {self.transactions / self.n:.2f} µs\n'
            # f' \t      - TxHashNumbers: {self.tx_hash_numbers / self.n:.2f} µs\n'
            # f' \t      - BlockBodyIndices: {self.block_body_indices / self.n:.2f} µs\n'
            # f' \t      - TransactionBlock: {self.transaction_block / self.n:.2f} µs\n'
            # f' \t      - Commit (body): {self.commit / self.n:.2f} µs\n'
        )
        
    def report_block(self) -> str:
        block_body_latency = sum((
            self.get_next_tx_num, 
            self.tx_senders, 
            self.transactions, 
            self.tx_hash_numbers, 
            self.block_body_indices, 
            self.transaction_block, 
            self.commit)) / self.n if self.n else 0.0
        return (
            f' BlockBodyInsertionMetrics: {block_body_latency:.2f} µs\n'
            f' \t  - GetNextTxNum: {self.get_next_tx_num / self.n:.2f} µs\n'
            f' \t  - TxSenders: {self.tx_senders / self.n:.2f} µs\n'
            f' \t  - Transactions: {self.transactions / self.n:.2f} µs\n'
            f' \t  - TxHashNumbers: {self.tx_hash_numbers / self.n:.2f} µs\n'
            f' \t  - BlockBodyIndices: {self.block_body_indices / self.n:.2f} µs\n'
            f' \t  - TransactionBlock: {self.transaction_block / self.n:.2f} µs\n'
            f' \t  - Commit (body): {self.commit / self.n:.2f} µs\n'
        )
        
        
class BlockAppendMetrics:
    
    LOG_PATTERN = r'Appended blocks range=\d+..=\d+ actions=\[\(InsertHeaders, (\d+(?:\.\d+)?)([mnµs]+)\), \(InsertState, (\d+(?:\.\d+)?)([mnµs]+)\), \(InsertHashes, (\d+(?:\.\d+)?)([mnµs]+)\), \(InsertHistoryIndices, (\d+(?:\.\d+)?)([mnµs]+)\), \(UpdatePipelineStages, (\d+(?:\.\d+)?)([mnµs]+)\)\]'
    
    def __init__(self):
        self.n = 0
        
        self.insert_header = 0.0  # equals to the BlockInsertion latency
        self.insert_state = 0.0
        self.insert_hash = 0.0
        self.insert_history_indices = 0.0
        self.update_pipeline_stages = 0.0
        
    def update(self, *args):
        assert len(args) == 5
        self.n += 1
        self.insert_header += args[0]
        self.insert_state += args[1]
        self.insert_hash += args[2]
        self.insert_history_indices += args[3]
        self.update_pipeline_stages += args[4]
    
    def add(self, other):
        assert isinstance(other, BlockAppendMetrics)
        
        self.n += other.n
        self.insert_header += other.insert_header
        self.insert_state += other.insert_state
        self.insert_hash += other.insert_hash
        self.insert_history_indices += other.insert_history_indices
        self.update_pipeline_stages += other.update_pipeline_stages
        
    def extend(self, others):
        for other in others:
            self.add(other)
        return self
    
    def total_latency(self) -> float:
        return sum((
            self.insert_header,
            self.insert_state,
            self.insert_hash,
            self.insert_history_indices,
            self.update_pipeline_stages)) / self.n if self.n else 0.0
        
    def report_with(self, insertion: BlockInsertionMetrics) -> str:
        return (
            f' \t  - InsertHeader: {self.insert_header / self.n:.2f} µs\n'
            f'{insertion.report_header()}'  # BlockInsertionMetrics
            f' \t  - InsertState: {self.insert_state / self.n:.2f} µs\n'
            f' \t  - InsertHash: {self.insert_hash / self.n:.2f} µs\n'
            f' \t  - InsertHistoryIndices: {self.insert_history_indices / self.n:.2f} µs\n'
            f' \t  - UpdatePipelineStages: {self.update_pipeline_stages / self.n:.2f} µs\n'
        )
        
   
class CommitMetric:
    
    LOG_PATTERN = r'Commit total_duration=(\d+(?:\.\d+)?)([mnµs]+)'
    
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
    
    
class CanonicalizationMetrics:
    
    LOG_PATTERN = r'Canonicalization finished actions=\[\(CloneOldBlocks, (\d+(?:\.\d+)?)([mnµs]+)\), \(FindCanonicalHeader, (\d+(?:\.\d+)?)([mnµs]+)\), \(SplitChain, (\d+(?:\.\d+)?)([mnµs]+)\), \(SplitChainForks, (\d+(?:\.\d+)?)([mnµs]+)\), \(MergeAllChains, (\d+(?:\.\d+)?)([mnµs]+)\), \(UpdateCanonicalIndex, (\d+(?:\.\d+)?)([mnµs]+)\), \(RetrieveStateTrieUpdates, (\d+(?:\.\d+)?)([mnµs]+)\), \(CommitCanonicalChainToDatabase, (\d+(?:\.\d+)?)([mnµs]+)\)\]'
    
    def __init__(self):
        self.n = 0
        
        self.clone_old_blocks = 0.0
        self.find_canonical_header = 0.0
        self.split_chain = 0.0
        self.split_chain_forks = 0.0
        self.merge_all_chains = 0.0
        self.update_canonical_index = 0.0
        self.retrieve_state_trie_update = 0.0
        self.commit_cononical_chain_to_database = 0.0  # includes the BlockAppend latency
        
    def update(self, *args):
        assert len(args) == 8
        self.n += 1
        self.clone_old_blocks += args[0]
        self.find_canonical_header += args[1]
        self.split_chain += args[2]
        self.split_chain_forks += args[3]
        self.merge_all_chains += args[4]
        self.update_canonical_index += args[5]
        self.retrieve_state_trie_update += args[6]
        self.commit_cononical_chain_to_database += args[7]
        
    def add(self, other):
        assert isinstance(other, CanonicalizationMetrics)
        
        self.n += other.n
        self.clone_old_blocks += other.clone_old_blocks
        self.find_canonical_header += other.find_canonical_header
        self.split_chain += other.split_chain
        self.split_chain_forks += other.split_chain_forks
        self.merge_all_chains += other.merge_all_chains
        self.update_canonical_index += other.update_canonical_index
        self.retrieve_state_trie_update += other.retrieve_state_trie_update
        self.commit_cononical_chain_to_database += other.commit_cononical_chain_to_database
        
    def extend(self, others):
        for other in others:
            self.add(other)
        return self
        
    def report_with(self, append: BlockAppendMetrics, insertion: BlockInsertionMetrics) -> str:
        total_latency = sum((
                self.clone_old_blocks,
                self.find_canonical_header,
                self.split_chain,
                self.split_chain_forks,
                self.merge_all_chains,
                self.update_canonical_index,
                self.retrieve_state_trie_update,
                self.commit_cononical_chain_to_database)) / self.n if self.n else 0.0
        return (
            f'{insertion.report_block()}'
            f' CanonicalizationMetrics: {total_latency:.2f} µs\n'
            f' \tCloneOldBlocks: {self.clone_old_blocks / self.n:.2f} µs\n'
            f' \tFindCanonicalHeader: {self.find_canonical_header / self.n:.2f} µs\n'
            f' \tSplitChain: {self.split_chain / self.n:.2f} µs\n'
            f' \tSplitChainForks: {self.split_chain_forks / self.n:.2f} µs\n'
            f' \tMergeAllChains: {self.merge_all_chains / self.n:.2f} µs\n'
            f' \tUpdateCanonicalIndex: {self.update_canonical_index / self.n:.2f} µs\n'
            f' \tRetrieveStateTrieUpdate: {self.retrieve_state_trie_update / self.n:.2f} µs\n'
            f' \tCommitCanonicalChainToDatabase: {self.commit_cononical_chain_to_database / self.n:.2f} µs\n'
            f'{append.report_with(insertion)}'
            f' \t  - Other(e.g., commit): {self.commit_cononical_chain_to_database/self.n - append.total_latency() :.2f}'
        )


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
