import win32com.client

# Inicia uma instância do Outlook
outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")

# Acessa a caixa de entrada padrão
inbox = outlook.GetDefaultFolder(6)  # 6 = inbox

# Pega os emails
messages = inbox.Items
for message in messages:
    print("Assunto:", message.Subject)
    print("Remetente:", message.SenderName)
    print("Data:", message.ReceivedTime)
    print("Corpo:", message.Body[:200], '\n')  # Print dos primeiros caracteres do corpo