## fonctionnemnet de la fonction:
## fonction qui prend un hash et un score et le range dans matching score

## parametre d'entree:
## hash: hash dont on a calcule le score
## score: score de mathcing du hash
## matching score: [[hash1, score1],[hash2, score2],[hash3, score3]]

## retour fonction::
## nouveau triplet des hash avec le score de matching le plus eleve

def sort_score(hash, score, matching_score):
    new_score = matching_score[:]

    ## score > score1
    if (score > matching_score[0][1]):
        ## remplacement de hash3 et score3 par hash2 et score2
        new_score[2][0] = new_score[1][0]
        new_score[2][1] = new_score[1][1]
        ## remplacement de hash2 et score2 par hash1 et score1
        new_score[1][0] = new_score[0][0]
        new_score[1][1] = new_score[0][1]
        ## remplacement de hash1 et score1 par score et hash
        new_score[0][0] = hash
        new_score[0][1] = score

    ## score > score2
    elif (score > matching_score[1][1]):
        ## remplacement de hash3 et score3 par hash2 et score2
        new_score[2][0] = new_score[1][0]
        new_score[2][1] = new_score[1][1]
        ## remplacement de hash2 et score2 par hash et score
        new_score[1][0] = hash
        new_score[1][1] = score

    ## score > score3
    elif (score > matching_score[2][1]):
        ## remplacement de hash3 et score3 par hash et score
        new_score[2][0] = hash
        new_score[2][1] = score

    return new_score



## fonctionnement de la fonction:
## fonction qui compare la position dans le dictionnaire de deux chaine de caractères

## parametre d'entree:
## str1: chaine de caractere
## str2: chaine de caractere

## retour fonction:
## 1 si str1 avant str2
## 0 si str1 == str2
## -1 si str1 après str2
## -2 cas exceptionnel

def calcul_dist_ascii(str1, str2):
    i = 0;

    while ((i < len(str1)) and (i < len(str2)) and (str1[i] == str2[i])):
        i += 1

    ## cas: "aaaaa" "aaaaa"
    if ((i == len(str1)) and (i == len(str2))):
        return 0

    ## cas: "aaaab" "aaaac"
    elif ((i < len(str1)) and (i < len(str2)) and (str1[i] < str2[i])):
            return 1

    ## cas: "aaaac" "aaaab"
    elif ((i < len(str1)) and (i < len(str2)) and (str1[i] > str2[i])):
            return -1

    ## cas: "aaaa" "aaaaa"
    elif (len(str1) < len(str2)):
        return 1

    ## cas: "aaaaa" "aaaa"
    elif (len(str1) > len(str2)):
        return -1



    return -2



## fonctionnement de la fonction:
## fonction qui calcul le score de matching entre deux signatures
## en calculant la proportion d'item present dans les deux signatures

## parametre d'entree:
## clear_signature: signature de l'utilisateur de la base clear
## anon_signature: signature d'un hash de la base anonymisee

## retour fonction:
## score de matching entre 0 et 1

def calcul_matching_score(clear_signature, anon_signature):
    nb_matching_item = 0

    for item in clear_signature:
        i = 0
        while (i < len(anon_signature)):
            if (item == anon_signature[i]):
                nb_matching_item += 1
                break
            i += 1

    return (2 * nb_matching_item) / (len(clear_signature) + len(anon_signature))
