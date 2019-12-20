from utilities.py import *

## fonctionnement de la fonction:
## fonction qui prend en entrée deux liste de signature TRIEES par taille
## de signature croissante et qui retourne un dictionnaire qui a chaque clé
## id_user associe les trois hash dont les signatures matchent au mieux la sienne

## parametre d'entree:
## clear_signature: [[id_user1, signature1, taille], ..., [id_userN, signatureN, taille]]
## anon_signature: [[hash1, signature1, taille], ..., [hashN, signatureN, taille]]
## seuil: float / int : seuil minimal pour concidere un match quasi parfait
##  diff_taille: différence maximale toleree entre la signature clear et hash

## retour fonction:
## reponse = {id_user1: [[hash1, score1],[hash2, score2],[hash3, score3]]}
## impossible ==> list [[id_user1, [[hash1, score1],[hash2, score2],[hash3, score3]]], ....]

def match_hash_to_user(clear_signature, anon_signature, seuil, diff_taille):
    ## initialisation variables
    anon_size = len(anon_signature)
    resultat_matching = []
    init_score_hash = [["hash1", 0], ["hash2", 0], ["hash3", 0]]
    save_score_hash = {}
    index = 0

    ## parcours des differentes signatures dans clear_signature
    for signature in clear_signature:

        ## cas ou l'index pointe sur une signature anon plus petite
        ## => on obtient l'index de la premiere signature de taille superieur ou egale
        while ((index < anon_size - 1) and (anon_signature[index][2] < signature[2])):
            index += 1;

        ## cas ou l'index pointe sur une signature anon plus grande
        ## ==> on obtient l'index de la premiere signature de taille inferieure ou egale
        while ((index > 0) and (anon_signature[index][2] > signature[2])):
            index -= 1

        ## initialisation score hash
        matching_score = init_score_hash[:]



        ## parcours des signatures plus petites que celle de l'utilisateur
        ## parcours dans le sens des tailles décroissantes
        ## => plus de lignes supprimees que de lignes ajoutées (ou lignes pas supprimées)
        i = index
        while ((i >= 0) and (matching_score[0][1] < seuil) and (((signature[2] - anon_signature[i][2]) ** 2) ** (1/2) < diff_taille)):
            ## calcul du score de matching (seulement si le hash n'a pas deja ete atttribue avec certitude)
            if not ((save_score_hash.has_key(anon_signature[i][0]) and (save_score_hash[anon_signature[i][0]] >= seuil))):
                score =  calcul_matching_score(signature[1], anon_signature[i][1])
                matching_score = sort_score(anon_signature[i][0], score, matching_score)
                """
                """
                save_score_hash[anon_signature[i][0]] = score
            i -= 1

        ## sauvegarde de l'index pour la signature "trop petite"
        if ((((signature[2] - anon_signature[i][2]) ** 2) ** (1/2) < diff_taille)):
            save_index_trop_petit = i



        ## parcours des signatures plus grandes que celle de l'utilisateur
        ## parcours dans le sens des tailles croissantes
        ## => plus de lignes ajoutees que de lignes supprimees
        i = index + 1
        while ((i < anon_size) and (matching_score[0][1] < seuil) and (((signature[2] - anon_signature[i][2]) ** 2) ** (1/2) < diff_taille)):
            ## calcul du score de matching (seulement si le hash n'a pas deja ete atttribue avec certitude)
            if not ((save_score_hash.has_key(anon_signature[i][0]) and (save_score_hash[anon_signature[i][0]] >= seuil))):
                score =  calcul_matching_score(signature[1], anon_signature[i][1])
                matching_score = sort_score(anon_signature[i][0], score, matching_score)
                save_score_hash[anon_signature[i][0]] = score
            i += 1

        ## sauvegarde de l'index pour la signature "trop grande"
        if ((((signature[2] - anon_signature[i][2]) ** 2) ** (1/2) < diff_taille)):
            save_index_trop_grand = i



        ## parcours des signatures consideree "trop petites"
        ## parcours dans le sens das tailles decroissantes
        i = save_index_trop_petit
        while ((i >= 0) and (matching_score[0][1] < seuil)):
            ## calcul du score de matching (seulement si le hash n'a pas deja ete atttribue avec certitude)
            if not ((save_score_hash.has_key(anon_signature[i][0]) and (save_score_hash[anon_signature[i][0]] >= seuil))):
                score =  calcul_matching_score(signature[1], anon_signature[i][1])
                matching_score = sort_score(anon_signature[i][0], score, matching_score)
                save_score_hash[anon_signature[i][0]] = score
            i -= 1


        ## parcours des signatures consideree "trop grandes"
        ## parcours dans le sens das tailles croissantes
        i = save_index_trop_grand
        while ((i < anon_size) and (matching_score[0][1] < seuil)):
            ## calcul du score de matching (seulement si le hash n'a pas deja ete atttribue avec certitude)
            if not ((save_score_hash.has_key(anon_signature[i][0]) and (save_score_hash[anon_signature[i][0]] >= seuil))):
                score =  calcul_matching_score(signature[1], anon_signature[i][1])
                matching_score = sort_score(anon_signature[i][0], score, matching_score)
                save_score_hash[anon_signature[i][0]] = score
            i += 1

        ## ajout du resultat dans resultat_matching
        resultat_matching.append([signature[0], matching_score])

    return resultat_matching
