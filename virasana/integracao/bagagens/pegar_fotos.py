import os
import shutil

caminho_fotos = r'\\fileserver\ALFSTS\Grupos\DIREP\EQVIG\III. FÁBIO\JANAINA DE ALMEIDA LOPES CECILIO - MRSU3114889 - FICHA 1996\FOTOS'
nome = 'C:/JANAINA-1996'

os.mkdir(nome)

for pasta in os.listdir(caminho_fotos):
    for imagem in os.listdir(os.path.join(caminho_fotos, pasta)):
        shutil.copyfile(os.path.join(caminho_fotos, pasta, imagem),
                        os.path.join(nome, imagem))
