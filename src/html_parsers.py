import re
import string
import unicodedata

from bs4 import BeautifulSoup


def get_item(parsed_l, substring):
    r = [idx for idx, s in enumerate(parsed_l) if substring in s]
    if r:
        return r
    else:
        return None


def get_fields_20F(html_bytes, parse_field):

    soup = BeautifulSoup(html_bytes, "lxml")
    soup1 = soup.select(parse_field)
    original_l, parsed_l = [], []
    for i in soup1:
        decoded = re.sub(
            "<[^<]+?>",
            "",
            unicodedata.normalize("NFKD", str(i))
            .encode("ascii", "ignore")
            .decode("utf-8"),
        )
        decoded = re.sub(" +", " ", decoded)
        decoded = decoded.lstrip().rstrip()
        if decoded:
            original_l.append(decoded)
            parsed_l.append(
                decoded.translate(str.maketrans("", "", string.punctuation)).lower()
            )

    company_name = get_item(parsed_l, "exact name")

    company_name = (
        get_item(parsed_l, "exactnname") if not company_name else company_name
    )

    company_name = (
        get_item(parsed_l, "exacttname") if not company_name else company_name
    )

    company_name = (
        get_item(parsed_l, "exactn name") if not company_name else company_name
    )
    company_name = (
        get_item(parsed_l, "exact nname") if not company_name else company_name
    )

    company_name = (
        get_item(parsed_l, "exactt name") if not company_name else company_name
    )
    company_name = (
        get_item(parsed_l, "exact tname") if not company_name else company_name
    )

    if company_name:
        company_name = company_name[0]

    comission_file_number = get_item(parsed_l, "sion file")

    comission_file_number = (
        get_item(parsed_l, "sionnfile")
        if not comission_file_number
        else comission_file_number
    )

    comission_file_number = (
        get_item(parsed_l, "siontfile")
        if not comission_file_number
        else comission_file_number
    )

    comission_file_number = (
        get_item(parsed_l, "sionn file")
        if not comission_file_number
        else comission_file_number
    )
    comission_file_number = (
        get_item(parsed_l, "sion nfile")
        if not comission_file_number
        else comission_file_number
    )

    comission_file_number = (
        get_item(parsed_l, "siont file")
        if not comission_file_number
        else comission_file_number
    )
    comission_file_number = (
        get_item(parsed_l, "sion tfile")
        if not comission_file_number
        else comission_file_number
    )

    if comission_file_number:
        comission_file_number = comission_file_number[0]

    address = get_item(parsed_l, "dress")

    if address:
        address = address[0]

    start_item_1 = get_item(parsed_l, "information on the company")

    start_item_1 = (
        get_item(parsed_l, "informationnon the company")
        if not start_item_1
        else start_item_1
    )
    start_item_1 = (
        get_item(parsed_l, "information onnthe company")
        if not start_item_1
        else start_item_1
    )
    start_item_1 = (
        get_item(parsed_l, "information on thencompany")
        if not start_item_1
        else start_item_1
    )

    start_item_1 = (
        get_item(parsed_l, "informationton the company")
        if not start_item_1
        else start_item_1
    )
    start_item_1 = (
        get_item(parsed_l, "information ontthe company")
        if not start_item_1
        else start_item_1
    )
    start_item_1 = (
        get_item(parsed_l, "information on thetcompany")
        if not start_item_1
        else start_item_1
    )

    start_item_1 = (
        get_item(parsed_l, "informationn on the company")
        if not start_item_1
        else start_item_1
    )
    start_item_1 = (
        get_item(parsed_l, "information non the company")
        if not start_item_1
        else start_item_1
    )
    start_item_1 = (
        get_item(parsed_l, "information onn the company")
        if not start_item_1
        else start_item_1
    )
    start_item_1 = (
        get_item(parsed_l, "information on nthe company")
        if not start_item_1
        else start_item_1
    )
    start_item_1 = (
        get_item(parsed_l, "information on then company")
        if not start_item_1
        else start_item_1
    )
    start_item_1 = (
        get_item(parsed_l, "information on the ncompany")
        if not start_item_1
        else start_item_1
    )

    if start_item_1:
        start_item_1 = start_item_1[0]

    return (
        [company_name, comission_file_number, address, start_item_1],
        parsed_l,
        original_l,
    )


def get_fields_10K(html_bytes, parse_field):

    soup = BeautifulSoup(html_bytes, "lxml")
    soup1 = soup.select(parse_field)
    original_l, parsed_l = [], []
    for i in soup1:
        decoded = re.sub(
            "<[^<]+?>",
            "",
            unicodedata.normalize("NFKD", str(i))
            .encode("ascii", "ignore")
            .decode("utf-8"),
        )
        decoded = re.sub(" +", " ", decoded)
        decoded = decoded.lstrip().rstrip()
        if decoded:
            original_l.append(decoded)
            parsed_l.append(
                decoded.translate(str.maketrans("", "", string.punctuation)).lower()
            )

    company_name = get_item(parsed_l, "exact name")

    company_name = (
        get_item(parsed_l, "exactnname") if not company_name else company_name
    )

    company_name = (
        get_item(parsed_l, "exacttname") if not company_name else company_name
    )

    company_name = (
        get_item(parsed_l, "exactn name") if not company_name else company_name
    )
    company_name = (
        get_item(parsed_l, "exact nname") if not company_name else company_name
    )

    company_name = (
        get_item(parsed_l, "exactt name") if not company_name else company_name
    )
    company_name = (
        get_item(parsed_l, "exact tname") if not company_name else company_name
    )

    if company_name:
        company_name = company_name[0]

    comission_file_number = get_item(parsed_l, "sion file")

    comission_file_number = (
        get_item(parsed_l, "sionnfile")
        if not comission_file_number
        else comission_file_number
    )

    comission_file_number = (
        get_item(parsed_l, "siontfile")
        if not comission_file_number
        else comission_file_number
    )

    comission_file_number = (
        get_item(parsed_l, "sionn file")
        if not comission_file_number
        else comission_file_number
    )
    comission_file_number = (
        get_item(parsed_l, "sion nfile")
        if not comission_file_number
        else comission_file_number
    )

    comission_file_number = (
        get_item(parsed_l, "siont file")
        if not comission_file_number
        else comission_file_number
    )
    comission_file_number = (
        get_item(parsed_l, "sion tfile")
        if not comission_file_number
        else comission_file_number
    )

    if comission_file_number:
        comission_file_number = comission_file_number[0]

    address = get_item(parsed_l, "principal executive")

    address = get_item(parsed_l, "principalnexecutive") if not address else address

    address = get_item(parsed_l, "principaltexecutive") if not address else address

    address = get_item(parsed_l, "principaln executive") if not address else address
    address = get_item(parsed_l, "principal nexecutive") if not address else address

    address = get_item(parsed_l, "principalt executive") if not address else address
    address = get_item(parsed_l, "principal texecutive") if not address else address

    if address:
        address = address[0]

    start_item_1 = get_item(parsed_l, "business")

    if start_item_1:
        start_item_1 = start_item_1[0]

    return (
        [company_name, comission_file_number, address, start_item_1],
        parsed_l,
        original_l,
    )
