#[derive(Clone, Debug)]
pub struct ChordNodeService<I, T> {
    id: I,
    local_node: NodeMeta<I>,
    query_engine: QueryEngine<I, T>,
    command_engine: CommandEngine<I, T>,
    forwarding_engine: ForwardingEngine<I, T>,
}
