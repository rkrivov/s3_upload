#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
import os


# The notifier function
def notify(message: str, title: str = None, subtitle: str = None):
    arguments = []

    if title is not None and title != '':
        t = '-title {!r}'.format(title)
        arguments.append(t)

    if subtitle is not None and subtitle != '':
        s = '-subtitle {!r}'.format(subtitle)
        arguments.append(s)

    m = '-message {!r}'.format(message)
    arguments.append(m)

    os.system('terminal-notifier {}'.format(' '.join(arguments)))
