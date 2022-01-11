# Sistema de Arquivos
Sistema desenvolvido para a matéria de Sistemas Operacionais, ele visa simular os comandos de Linux para gerenciamento de arquivos.

# Instalação
É necessário o [Python](https://www.python.org/) (versão >= 3.8) para execução. Tendo o Python instalado, basta realizar o download do código fonte e executar.

No Linux:
```
python3 main.py -u usuario
```
No Windows:
```
py main.py -u usuario
```

# Comandos
| Comando | Função |
| ------- | ------ |
| ```cd``` | Muda o diretório atual.|
| ```ls``` | Lista todos os arquivos e subdiretórios no diretório atual |
| ```mkdir diretorio``` | Cria um diretório.| 
| ```rmdir diretorio``` | Remove um diretório. O diretório especificado deve estar vazio.|
|```mv caminho nome``` | Renomeia um diretório ou arquivo.|
|```mvdir caminho1 caminho2```| Move um arquivo ou diretório.|
|```touch arquivo```| Cria um arquivo.|
|```rm arquivo```| Remove arquivo.|
|```echo "conteudo" >> arquivo``` | Escreve ```conteudo``` no ```arquivo```.|
|```cat arquivo``` | Lê o conteúdo de ```arquivo``` e exibe na tela.|
|```cp arquivo1 arquivo2```| Copia o conteúdo de ```arquivo1``` para ```arquivo2```. Se ```arquivo2``` não existir, será criado, e se já existir outro arquivo com o mesmo nome, sobrescreverá.|

# Exemplos
Criar diretório:
```
mkdir p1
mkdir p2
mkdir p3
```
Remover diretório (precisa estar vazio):
```
rmdir p3
```
Trocar de diretório:
```
cd p2
cd ..
```
Renomear diretório:
```
mv p1 p3
```
Mover diretório:
```
mvdir p2 p3
```
Criar arquivo:
```
touch a.txt
```
Escrever em arquivo:
```
echo "texto" >> a.txt
```
Exibir texto do arquivo:
```
cat a.txt
```
