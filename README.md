# Desafio Final de Dados Lighthouse by Indicium

## Contexto do projeto

O objetivo do projeto é auxiliar a empresas Adventure Works a melhorar o entendimento sobre seus dados, utilizando-os de forma estratégica, 
mantendo e melhorando seu ritmo de crescimento, nortenando suas decisões para se tornar uma empresa data driven. A adventure works, uma industria 
de bicicletas que possui em seu portifólio mais de 500 produtos distindos, atendendo seus 20.000 clientes em todas as suas necessidades, fornecendo desde 
bicicletas, ventimentas e acessórios.

A primeira etapa do projeto é dividida em duas frentes, sendo a primeira delas votada ao entendimento dos indicadores do setor de vendas (sales), gerados a 
partir de registros de vendas diretas ou para lojas parceiras. A segunda frente tem por objetivo auxiliar o setor de Planejamento de Demanda a melhrorar seu 
planejamento de produção, utilizando dados estatisticos e análises preditivas, com grande potencial a se tornar um projeto de bastante relevancia para a empresa.

Dentre os principais envolvidos com o projeto podemos listar:

|  Nome de colaborador |    Cargo atual  |
|-----------------------|---------------|
| Carlos Silveira     |  CEO     |
|  João Muller        |   Diretor de Inivação |
|  Silvana Teixeira  |  Diretora Comercial|
| Nilson Ramos  |  Diretor de TI  |
| Gabriel Santos  |  Analista de TI  |
|Luís Soares  |   Gestor de Planejamento de Demanda |

## Dados disponíveis 

A Adventure Works possui um banco de dados transacional (PostgreSQL) que armazena dados de diferentes áreas. Os dados estão distribuidoes em 68 tabelas 
divididas em 5 schemas:
- HR (recursos humanos)
- Sales (vendas)
- Person (pessoas)
- Production (produção)
- Purchasing (compras)

![Diagrama banco de dados transacional](https://drive.google.com/file/d/1ntVGGWODYUuMge4GzrU66pGlwybdafHb/view?usp=sharing)

Você pode acessar o diagrama completo cliando [aqui](https://drive.google.com/file/d/1ntVGGWODYUuMge4GzrU66pGlwybdafHb/view?usp=sharing).

## Infraestrutura Moderna de Dados

Seguindo as boas práticas conforme a metodologia do Modern Analytics Stack, adotamos a utilização de frameworks e ferramentas reconhecidas, 
que atenderiam nossas necessidades em cada etapa do ELT (Extract / Load / Transform)

Para atender as necessidas foram empregadas as seguintes ferramentas:
- `GitHub` - É uma plataforma de armazenamento e compartilhamento de código, permitindo que desenvolvedores trabalhem juntos em projetos. 
Seus principais benefícios são o controle de versões, que registra todas as mudanças, a colaboração fácil entre equipes, a revisão de 
código para garantia da qualidade das alterações.

- `Google BigQuery` - Serviço de armazenamento em nuvem da Google, utilizado como data warehouse, que permite armazenar e analisar 
grandes volumes de dados de forma rápida e eficiente, permitindo a escalabilidade para lidar com grandes conjuntos de dados,
armazenamento seguro e confiável, e fácil integração com diversas ferramentas de análise e visualização de dados.

- `dbt` -  Utilizada como ferramenta de transformação de dados, sendo de fácil interpretação através da aplicação de linguagem SQL durante 
as transformações. Permitindo o versionamento de código, automação de testes de dados e documentação integrada é considerada o core dentro do processo de transformação. 

- `PowerBI` - É uma ferramenta de business intelligence da Microsoft que permite a visualização e análise de dados. Seus principais benefícios incluem 
a criação de dashboards interativos, relatórios dinâmicos, integração fácil com diversas fontes de dados, e capacidades de compartilhamento segura e eficiente.

# Extract and Load

O processo de extração se deu com a cópia (comando `fork`) dos dados contidos neste [repositório](https://github.com/techindicium/academy-dbt) para nosso ambiente local.

Integrando os ambientes do `dbt` com o `GitHub` e `BigQuery`, através do comando `dbt seed` é realizado o carregamento dos dados para nosso armazenamento em nuvem, finalizando a etapa de Extract and Load.

## Transform

Após o entendimento das regras de negócio, é realizado a exploração dos dados, identificando as informações necessárias para obtenção das respostas que o time de negocios espera.
Com os dados identificados, é possivel desenvolver o diagrama conceitual e logico, que interliga as informações necessárias para 

imagem

O processo de transformação é subdividido em três principais etapas, descritas como camadas de transformação, são elas:

- `Staging`: Na camada de Staging é realizada a primeira limpesa dos dados, selecionando as colunas necessárias em cada tabela, alterando os data types, renomeação das colunas e remoção de registros nulas e duplicadas.

- `Intermediate` - Na camada intermediária de transformação, são realizadas as uniões (joins) entre as tabelas, sendo a base do modelo final.

- `marts` - Na camada marts são aplicadas as regras de negócio, inclusão de métricas e definição final do modelo. Ainda na camada marts, cada tabela da origem a um
arquivo `.yml` onde são documentadas todas as colunas finais do modelo e a implementação de testes que garantem a qualidade do modelo.


## Vizualização de dados 

Com os dados transformados e organizados no data warehouse, realizamos a integração com o Power BI através do método de Importação, que armazena os dados em cache na memória e envia consultas para os dados armazenados.

Após carregamento é realizado a integração entre as tabelas garantindo que os relacionamentos entre as chaves primárias e estrangeiras estejam conforme modelagem. 

Você pode verificar o resultado do dashboard através deste Link. 
