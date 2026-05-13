export function createState() {
  return {
    localVirtualNode: null,
    localProfile: null,
    sessionsByVirtualNodeId: {},
    contacts: [
      {
        virtual_node_id: "friend-demo-001",
        display_name: "Lia",
        public_key: null,
        status: "online",
      },
      {
        virtual_node_id: "friend-demo-002",
        display_name: "Nico",
        public_key: null,
        status: "away",
      },
    ],
    directMessages: [],
    feedPosts: [
      {
        author_name: "Lia",
        author_virtual_node_id: "friend-demo-001",
        text: "Testando publicacoes no feed descentralizado. A ideia e simples: perfil aponta para o estado atual e a rede entrega o resto.",
        created_at: new Date(Date.now() - 1000 * 60 * 8).toISOString(),
      },
      {
        author_name: "Nico",
        author_virtual_node_id: "friend-demo-002",
        text: "Quando a sessao virtual estiver ativa, mensagem direta entra como evento em tempo real.",
        created_at: new Date(Date.now() - 1000 * 60 * 27).toISOString(),
      },
    ],
    events: [],
  };
}
