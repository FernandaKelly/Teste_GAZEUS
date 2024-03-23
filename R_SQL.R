# Instala os pacotes necessários
install.packages(c('dplyr', 'dbplyr', 'RSQLite', 'readr', 'stringr'))

# Carrega os pacotes
library("dplyr")
library("dbplyr")
library("RSQLite")
library("readr")
library("stringr")


# Este curso utilizará como fonte de dados, um banco SQLite!

# O código abaixo carrega o CSV disponível no site da ANS (caso fossemos acessar os dados do arquivo CSV).
# igr <- read_csv2('data/dados-gerais-das-reclamacoes-por-operadora.csv', locale=locale(encoding='Latin1'))


# No DB Browser for SQLite criei o igr.db com base no CSV da ANS

# Conexão do SQLite no R
con <- DBI::dbConnect(RSQLite::SQLite(), "data/igr.db")

# Lista as tabelas do banco igr.db
dbListTables(con)

# A tabela igr contém todas as observações do arquivo CSV
# No DB Browser for SQLite fiz um tratamento e criei duas tabelas a partir da igr, são elas:
# - tabela igr_2019: contém somente as observações com data de atendimento em 2019
# - tabela igr_2020: contem somente as observações com data de atendimento em 2020
# As duas novas tabelas serão utilizadas no tópico de joins


# Relembrando:
# 1. Os comandos na sintaxe R (dplyr) desse material também podem ser executados na leitura 
# de arquivos CSV, XLS, etc. Porém, nesse curso, rodaremos os comandos 
# acessando um banco de dados SQLite.

# 2. Os comandos na sintaxe SQL desse material também podem ser executados 
# no próprio banco de dados SQL (por exemplo, SQL Server, MySQL, etc).
# Mas atenção, eles podem variar um pouco, depende do banco de dados utilizado.


# Vamos começar, vem comigo! :)

# Cria um dataframe (df) no R baseado na tabela igr do banco SQLite
df <- tbl(con, "igr") #lazziness, não carrega os dados do SQLite em um dataframe.
is.data.frame(df)
df <- df %>% collect() # coleta os dados, agora sim temos um dataframe.
is.data.frame(df)

# A partir de agora iremos nos referir ao dataframe igr_r 
# para as consultas e manipulações usando a sintaxe R (dplyr).

# Utilizaremos o seguinte esquema de equivalência de comandos SQL e R:
# De-para: Instrução SQL --> instrução dplyr


# De-para: SELECT --> View

igr_sql <- dbGetQuery(con, "SELECT * FROM igr") %>% View()

igr_r = df
View(igr_r) 

# View: outra opção
igr_r %>% View



# Dica: Para visualizar os dados além do console, use %>% View ao final das instruções.


#--------------------------------------------------------------------------

# De-para: SELECT --> select

# igr_sql e igr_r são os nomes dos dataframes que utilizarei para armazenar os dados a partir de agora.

igr_sql <- dbGetQuery(con, "SELECT razao_social, beneficiarios FROM igr")

igr_r <- df %>% 
  select(razao_social, beneficiarios) 


#--------------------------------------------------------------------------

# De-para: SELECT DISTINCT --> distinct

igr_sql <- dbGetQuery(con, "SELECT DISTINCT razao_social FROM igr")

igr_r <- df %>% distinct(razao_social)


#--------------------------------------------------------------------------

# De-para: SELECT TOP --> head

igr_sql <- dbGetQuery(con, "SELECT * FROM igr LIMIT 200")

igr_r <- df %>% head(200)


#--------------------------------------------------------------------------

# De-para: WHERE --> filter

igr_sql <- dbGetQuery(con, "SELECT razao_social, beneficiarios 
                            FROM igr WHERE beneficiarios > 1000000")

igr_r <- df %>% 
  select(razao_social, beneficiarios) %>% 
  filter(beneficiarios > 1000000)


#--------------------------------------------------------------------------

# De-para: LIKE -->  str_detect

igr_sql <- dbGetQuery(con, "SELECT * FROM igr
                            WHERE razao_social LIKE '%SAUDE%'")

igr_r <- df %>% filter(str_detect(razao_social, "SAUDE"))


#--------------------------------------------------------------------------

# De-para: IN --> %in%

igr_sql <- dbGetQuery(con, "SELECT * FROM igr
                            WHERE subtema_demanda IN ('Reembolso', 'Carência')")

igr_r <- df %>% 
  filter(subtema_demanda %in% c("Reembolso", "Carência")) 


#--------------------------------------------------------------------------

# De-para: NOT IN   ...   %!in% 

igr_sql <- dbGetQuery(con, "SELECT * FROM igr
                    WHERE subtema_demanda NOT IN ('Reembolso', 'Carência')")

# É preciso criar uma função para negar o operador in:
`%!in%` = Negate(`%in%`)

igr_r <- df %>% 
  filter(subtema_demanda %!in% c("Reembolso", "Carência")) 


#--------------------------------------------------------------------------

# De-para: MIN, MAX, AVG --> summarize(min, max, mean)
# Dica: summarise também funciona

igr_sql <- dbGetQuery(con, "SELECT MIN(beneficiarios) FROM igr")

igr_r <- df %>% summarize(min(beneficiarios))

igr_sql <- dbGetQuery(con, "SELECT MAX(beneficiarios) FROM igr")

igr_r <- df %>% summarize(max(beneficiarios)) 

igr_sql <- dbGetQuery(con, "SELECT AVG(beneficiarios) FROM igr")

igr_r <- df %>% summarize(mean(beneficiarios))


#--------------------------------------------------------------------------

# De-para: COUNT, COUNT DISTINCT --> n, n_distinct

# Conta todos as observações

igr_sql <- dbGetQuery(con, "SELECT COUNT(*) FROM igr")

igr_r <- df %>% summarize(n()) 


# Conta valores distintos da coluna

igr_sql <- dbGetQuery(con, "SELECT COUNT(DISTINCT(razao_social)) FROM igr")

igr_r <- df %>% summarize(n_distinct(razao_social)) %>% View


#--------------------------------------------------------------------------

# De-para: IS NULL --> is.na

igr_sql <- dbGetQuery(con, "SELECT * FROM igr WHERE registro_ans IS NULL")

igr_r <- df %>% filter(is.na(registro_ans)) 


#--------------------------------------------------------------------------

# De-para: IS NOT NULL --> !is.na

igr_sql <- dbGetQuery(con, "SELECT * FROM igr
                            WHERE registro_ans IS NOT NULL")

igr_r <- df %>% filter(!is.na(registro_ans))  


#--------------------------------------------------------------------------

# De-para: GROUP BY --> group_by

igr_sql <- dbGetQuery(con, "SELECT razao_social, 
                                   count(razao_social) as num_ben 
                            FROM igr
                            GROUP BY razao_social")

igr_r <- df %>% 
  group_by(razao_social) %>% 
  summarize(num_ben = n())


#--------------------------------------------------------------------------

igr_sql <- dbGetQuery(con, "SELECT razao_social, 
                                    count(razao_social) as qtd
                            FROM igr
                            GROUP BY razao_social
                            ORDER BY qtd")

# De-para: ORDER BY --> arrange
igr_r <- df %>% 
  group_by(razao_social) %>% 
  summarize(num_ben = n()) %>% 
  arrange(num_ben)


# ORDER BY DESC   ...   arrange(desc

igr_sql <- dbGetQuery(con, "SELECT razao_social, 
                                   count(razao_social) as qtd 
                            FROM igr
                            GROUP BY razao_social
                            ORDER BY qtd DESC")

igr_r <- df %>% 
  group_by(razao_social) %>% 
  summarize(num_ben = n()) %>% 
  arrange(desc(num_ben))


#--------------------------------------------------------------------------

# De-para: INSERT --> add_row

# Atenção: este comando insere uma observação no banco SQLite.
dbExecute(con, "INSERT INTO igr 
                VALUES(111111, 'dado2', 3, 4 
                     , '20/04/2020 14:30:32', 'dado6', 'dado7'
                     , 'dado8', 202007, '16/05/2020 16:00:03')")

# Vamos conferir se a nova observação está na base
igr_sql <- dbGetQuery(con, "SELECT * FROM igr
                            WHERE registro_ans = 111111")

# Atenção: este comando insere uma observação no objeto df, mas não no banco SQLite.
df <- df %>% add_row(registro_ans = 111111, razao_social = "dado2", beneficiarios = 3, numero_demanda = 4, 
                     data_atendimento = "20/04/2020 14:30:32", classificacao = "dado6", natureza_demanda = "dado7", 
                     subtema_demanda = "dado8", competencia = 202007, data_atualizacao = "16/05/2020 16:00:03") 

# * Ponto de pesquisa: é possível adicionar uma observação em um banco SQLite via sintaxe dplyr?

#--------------------------------------------------------------------------

# De-para: UPDATE --> mutate

dbGetQuery(con, "UPDATE igr 
                SET razao_social = 'plano_saude_ABC', beneficiarios= 101101
                WHERE registro_ans = '111111'")

df <- df %>%
  mutate(razao_social=if_else(registro_ans==111111, "plano_saude_ABC", razao_social)) %>%  
  mutate(beneficiarios=if_else(registro_ans==111111, 222222, beneficiarios)) 


#--------------------------------------------------------------------------

# De-para: DELETE -->  filter

# Conferência: quantidade de observações com subtema_demanda = 'Reembolso'
igr_sql <- dbGetQuery(con, "SELECT count(subtema_demanda) qtd FROM igr
                            WHERE subtema_demanda = 'Reembolso'")

# Atenção: esse comando deleta observações do banco SQLite
dbExecute(con, "DELETE FROM igr 
                WHERE subtema_demanda  = 'Reembolso'")

df <- df %>% filter(subtema_demanda  != "Reembolso") 

# Para você pensar: por que no comando SQL utilizamos o operador = 
# e no comando R (dplyr) usamos o operador != ?


#--------------------------------------------------------------------------

# De-para: UNION --> union
# union: colunas iguais, remove observações duplicadas.

# Verificação das tabelas igr_2019 e igr_2020
igr_sql <- dbGetQuery(con, "SELECT * FROM igr_2019") 
igr_sql <- dbGetQuery(con, "SELECT * FROM igr_2020")

igr_sql_2019_2020 <- dbGetQuery(con, "SELECT * FROM igr_2019 
                                      UNION 
                                      SELECT * FROM igr_2020")

# Carrega tabelas do banco SQLite em dataframes
df_igr_2019 <- tbl(con, "igr_2019") %>% collect()
df_igr_2020 <- tbl(con, "igr_2020") %>% collect()

igr_r_2019_2020 <- df_igr_2019 %>% union(df_igr_2020)


#--------------------------------------------------------------------------

# De-para: UNION_ALL -->  union_all
# union_all: colunas diferentes, não remove observações duplicadas.

igr_sql_2019_2020 <- dbGetQuery(con, "SELECT * FROM igr_2019 
                                      UNION ALL
                                      SELECT * FROM igr_2020")

igr_r_2019_2020 <- df_igr_2019 %>% union_all(df_igr_2020)


#--------------------------------------------------------------------------

# De-para: INNER JOIN --> inner_join

igr_sql_2019_2020 <- dbGetQuery(con, "SELECT * FROM igr_2019
                                      INNER JOIN igr_2020 ON 
                                      igr_2019.numero_demanda = igr_2020.numero_demanda")

igr_r_2019_2020 <- df_igr_2019 %>% inner_join(df_igr_2020, by=c("numero_demanda"))

#--------------------------------------------------------------------------

# De-para: LEFT JOIN --> left_join

igr_sql <- dbGetQuery(con, "SELECT * FROM igr_2019
                            LEFT JOIN igr_2020 ON 
                            igr_2019.numero_demanda = igr_2020.numero_demanda")

igr_r_2019_2020 <- df_igr_2019 %>% left_join(df_igr_2020, by=c("numero_demanda"))

#--------------------------------------------------------------------------

# De-para: RIGHT JOIN --> right_join

igr_sql <- dbGetQuery(con, "SELECT * FROM igr_2019
                            RIGHT JOIN igr_2020 ON 
                            igr_2019.numero_demanda = igr_2020.numero_demanda")

# Atenção --> Mensagem de erro: Error: RIGHT and FULL OUTER JOINs are not currently supported
# Portanto, o SQLite não suporta o RIGHT JOIN, mas outros bancos de dados têm esse comando.

# O dplyr possui o RIGHT JOIN
igr_r_2019_2020 <- df_igr_2019 %>% right_join(df_igr_2020, by=c("numero_demanda"))