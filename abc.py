import sys
import subprocess
import csv

def ensure_package(package: str):
    try:
        __import__(package)
    except ImportError:
        executable = sys.executable
        cmd = [
            executable, "-m", "pip", "install", package,
            "--trusted-host", "pypi.org",
            "--trusted-host", "files.pythonhosted.org",
            "--trusted-host", "pypi-official.argos-labs.com",
            "--disable-pip-version-check",
            "--no-cache-dir"
        ]

        subprocess.check_call(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT
        )

try:
    import pdfplumber
except ImportError:
    ensure_package("pdfplumber")
    import pdfplumber

dailyCSVRate = []
dailyRateDict = {}
crossCSVRate = []
crossRateDict = {}
crossSgdCSVRate = []
IsExisted = False


def truncate(num, decimalPlace=4):
    n = decimalPlace
    x = round(float(num) * (10**n), decimalPlace)
    integer = int(x)/(10**n)
    return integer


def getIndex(lineArray, country):
    index = -1
    try:
        countryWordList = country.split(' ')
        for word in countryWordList:
            index = lineArray.index(word)
    except ValueError:
        return -1
    return index


def getDailyCurrRate(lineArray, currencyCode, currencyInfo, _SGD_USD):
    global dailyRateDict
    index = getIndex(lineArray, currencyCode)

    if currencyCode == "Saudi Riyal" or currencyCode == "Indonesian Rupiah":
        if index >= 0:
            currency = currencyInfo[1]
            rate = \
                truncate((_SGD_USD / float(lineArray[index + currencyInfo[0]])) * currencyInfo[2], currencyInfo[3])
            dailyRateDict[currency] = rate
    else:
        if IsExisted == True:
            currency = currencyInfo[1]
            rate = \
                truncate((_SGD_USD / float(lineArray[index + currencyInfo[0]])) * currencyInfo[2], currencyInfo[3])
            dailyRateDict[currency] = rate


def getDailyCurrRateForOMR(currencyCode, OMRRate, currencyInfo, _SGD_USD):
    global dailyRateDict
    currency = currencyInfo[0]

    rate = \
        truncate((_SGD_USD / float(OMRRate)) * currencyInfo[1], currencyInfo[2])
    dailyRateDict[currency] = rate


def getCrossCurrRate(lineArray, currencyCode, currencyInfo):
    global dailyRateDict
    index = getIndex(lineArray, currencyCode)

    if currencyCode == "Saudi Riyal" or currencyCode == "Indonesian Rupiah":
        if index >= 0:
            currency = currencyInfo[1]
            rate = \
                truncate(float(lineArray[index + currencyInfo[0]]) * currencyInfo[2])
            crossRateDict[currency] = rate
    else:
        if IsExisted == True:
            currency = currencyInfo[1]
            rate = \
                truncate(float(lineArray[index + currencyInfo[0]]) * currencyInfo[2])
            crossRateDict[currency] = rate


def ConvertVal (lineArray):
    try:
        float(lineArray[0])
        return True
    except ValueError:
        return False
        
        
def startmain(wmPath, dailyCsvPath, crossCsvPath, crossSgdCsvPath, OMRRate):
    global dailyCSVRate
    global dailyRateDict
    global crossSgdCSVRate
    global IsExisted

    IsExisted = False
    wmCurrTbl = []
    startExtract = False
    dailyWmCountries = {
        'Australian Dollar': [1, 'AUD', 1, 4],
        'Chinese Yuan': [1, 'CNY', 100, 4],
        'Hong Kong Dollar': [1, 'HKD', 100, 4],
        'Indian Rupee': [1, 'INR', 100, 4],
        'Indonesian Rupiah': [1, 'IDR', 100, 5],
        'Japanese Yen': [10, 'JPY', 100, 4],
        'Malaysian Ringgit': [10, 'MYR', 100, 4],
        'Philippine Peso': [10, 'PHP', 100, 4],
        'Saudi Riyal': [1, 'SAR', 100, 4],
        'Korean Won': [19, 'KRW', 100, 4],
        'Taiwan Dollar': [19, 'TWD', 100, 4],
        'Thai Baht': [19, 'THB', 100, 5],
        'UAE Dirham': [19, 'AED', 100, 4],
        'Vietnamese Dong': [19, 'VND', 100, 5]
    }
    ftCurrTbl = []
    dailyFtCountries = {
        # 'Burma': [4, 'MMK', 100, 5],
        'Omani Rial': ['OMR', 1, 4]
    }
    crossFtCountries = {
        'Australian Dollar': [1, 'AUD', 1],
        # 'Burma': [4, 'MMK', 1],
        # 'Oman': [5, 'OMR', 1],
        'Singapore Dollar': [19, 'SGD', 1],
        'Chinese Yuan': [1, 'CNY', 1],
        'Hong Kong Dollar': [1, 'HKD', 1],
        'Indian Rupee': [1, 'INR', 1],
        'Indonesian Rupiah': [1, 'IDR', 1],
        'Japanese Yen': [10, 'JPY', 1],
        'Malaysian Ringgit': [10, 'MYR', 1],
        'Philippine Peso': [10, 'PHP', 1],
        'Saudi Riyal': [1, 'SAR', 1],
        'Korean Won': [19, 'KRW', 1],
        'Taiwan Dollar': [19, 'TWD', 1],
        'Thai Baht': [19, 'THB', 1],
        'UAE Dirham': [19, 'AED', 1],
        'Vietnamese Dong': [19, 'VND', 1],
        'Pound Sterling': [19,'USD_GBP',1],
        'Euro': [19, 'USD_EUR', 1]
    }
    
    # Open and retrieve all the value from "World Markets At A Glance" pdf report
    with pdfplumber.open(wmPath) as pdf:
        page = pdf.pages[0]
        lines = page.extract_text(x_tolerance=1, y_tolerance=0)

        for idx, line in enumerate(lines.split('\n')):
            if line.startswith('CURRENCIES'):
                startExtract = True

            if line.startswith('Rates are derived'):
                startExtract = False

            if startExtract:
                wmCurrTbl.append(line)

        # Get the Closing Mid value for Singapore (Dollar, Euro, Pound)
    for line in wmCurrTbl:
        lineArray = line.split(' ')
        index = getIndex(lineArray, 'Singapore')
        val = ConvertVal(lineArray)

        if index >= 0:
            if val == False:
                IsExisted = True
                continue

        if IsExisted == True:
            dailyRateDict['SGD_USD'] = truncate(lineArray[index + 19])
            dailyRateDict['SGD_EUR'] = truncate(lineArray[index + 22])
            dailyRateDict['SGD_GBP'] = truncate(lineArray[index + 25])
            IsExisted = False
            break

    _SGD_USD = dailyRateDict['SGD_USD']
    
    # Get DailyCurrencyRate data
    for currencyCode in dailyWmCountries.keys():
        for line in wmCurrTbl:
            lineArray = line.split(' ')

            if IsExisted == True:

                # Check twice for the next line
                val = ConvertVal(lineArray)
                if val == False:
                        continue

                getDailyCurrRate(lineArray, currencyCode, dailyWmCountries[currencyCode], _SGD_USD)
                IsExisted = False
                break

            index = getIndex(lineArray, currencyCode)
            if index >= 0:
                if currencyCode == "Saudi Riyal" or currencyCode == "Indonesian Rupiah":
                    getDailyCurrRate(lineArray, currencyCode, dailyWmCountries[currencyCode], _SGD_USD)
                    IsExisted = False
                    break
                else:
                    val = ConvertVal(lineArray)
                    if val == False:
                        IsExisted = True
                        continue


    # Get DailyCurrencyRate for OMR
    for currencyCode in dailyFtCountries.keys():
        getDailyCurrRateForOMR(currencyCode, OMRRate, dailyFtCountries[currencyCode], _SGD_USD)
    
    
    # Get CrossRate Data
    for currencyCode in crossFtCountries.keys():
        for line in wmCurrTbl:
            lineArray = line.split(' ')

            if IsExisted == True:

                # Check twice for the next line
                val = ConvertVal(lineArray)
                if val == False:
                    continue

                getCrossCurrRate(lineArray, currencyCode, crossFtCountries[currencyCode])
                IsExisted = False
                break

            index = getIndex(lineArray, currencyCode)
            if index >= 0:
                if currencyCode == "Saudi Riyal" or currencyCode == "Indonesian Rupiah":
                    getCrossCurrRate(lineArray, currencyCode, crossFtCountries[currencyCode])
                    IsExisted = False
                    break
                else:
                    val = ConvertVal(lineArray)
                    if val == False:
                        IsExisted = True
                        continue


    # Set the CrossRate for USD and OMR
    crossRateDict['USD_USD'] = float('1.00')
    crossRateDict['OMR'] = float(OMRRate)

    #daily_curr_order = ["AUD","SGD_EUR","SGD_GBP","OMR","SGD_USD","JPY","MYR","IDR","THB","INR","SAR","AED","PHP","HKD","CNY","TWD","KRW","VND","MMK"]
    daily_curr_order = ["AUD","SGD_EUR","SGD_GBP","OMR","SGD_USD","JPY","MYR","IDR","THB","INR","SAR","AED","PHP","HKD","CNY","TWD","KRW","VND"]
    for curr in daily_curr_order:
        dailyCSVRate.append(dailyRateDict[curr])

    #cross_curr_order = ["AUD","USD_EUR","USD_GBP","OMR","SGD","USD_USD","JPY","MYR","IDR","THB","INR","SAR","AED","PHP","HKD","CNY","TWD","KRW","VND","MMK"]
    cross_curr_order = ["AUD","USD_EUR","USD_GBP","OMR","SGD","USD_USD","JPY","MYR","IDR","THB","INR","SAR","AED","PHP","HKD","CNY","TWD","KRW","VND"]
    for curr in cross_curr_order:
        crossCSVRate.append(crossRateDict[curr])
        
    #cross_sgd_order = ["AUD","SGD_EUR","SGD_GBP","OMR","SGD","SGD_USD","JPY","MYR","IDR","THB","INR","SAR","AED","PHP","HKD","CNY","TWD","KRW","VND","MMK"]
    cross_sgd_order = ["AUD","SGD_EUR","SGD_GBP","OMR","SGD","SGD_USD","JPY","MYR","IDR","THB","INR","SAR","AED","PHP","HKD","CNY","TWD","KRW","VND"]
    for curr in cross_sgd_order:
        if curr == "SGD":
            crossSgdCSVRate.append(1)
        else:
            crossSgdCSVRate.append(dailyRateDict[curr])

    try:
        with open(dailyCsvPath, 'w', newline='') as dailyCsvFile:
            writer = csv.writer(dailyCsvFile)
            writer.writerow(dailyCSVRate)

        with open(crossCsvPath, 'w', newline='') as crossCsvFile:
            writer = csv.writer(crossCsvFile)
            writer.writerow(crossCSVRate)
        
        with open(crossSgdCsvPath, 'w') as crossSgdCsvFile:
            for rate in crossSgdCSVRate:
                crossSgdCsvFile.write(str(rate) + "\n")

    except IOError:
        print('I/O Error')


def main(wmPath, dailyCsvPath, crossCsvPath, crossSgdCsvPath, OMRRate):
    startmain(wmPath, dailyCsvPath, crossCsvPath, crossSgdCsvPath, OMRRate)


if __name__ == "__main__":
    wmPath = sys.argv[1] #World Markets at a glance.pdf
    #ftPath = sys.argv[2] #FT Guide to World Currencies.pdf
    dailyCsvPath = sys.argv[2] #dailyCurrencyRate.csv
    crossCsvPath = sys.argv[3] #crossCurrencyRate.csv
    crossSgdCsvPath = sys.argv[4] #crossCurrencyRate_SGD.csv
    OMRRate = sys.argv[5]  # crossCurrencyRate_SGD.csv
    try:
        main(wmPath, dailyCsvPath, crossCsvPath, crossSgdCsvPath, OMRRate)
    except Exception as e:
        print("check ==========", e)

    #wmPath = "C:\\Users\\snksea_rpa\\Desktop\\SANKYU RPA Argos Files\\Exchange Rate Daily Update\\Daily Exchange Rate_RPA\\Financial Times World Currencies\\2025\\11. 2025 Nov\\2025.11.11 World Markets at a glance.pdf"  # World Markets at a glance.pdf
    #ftPath = sys.argv[2] #FT Guide to World Currencies.pdf
    #dailyCsvPath = "C:\\Users\\snksea_rpa\\Desktop\\SANKYU RPA Python Files\\DailyExchangeRate\\dailyCurrencyRate.csv"  # dailyCurrencyRate.csv
    #crossCsvPath = "C:\\Users\\snksea_rpa\\Desktop\\SANKYU RPA Python Files\\DailyExchangeRate\\crossCurrencyRate.csv"  # crossCurrencyRate.csv
    #crossSgdCsvPath = "C:\\Users\\snksea_rpa\\Desktop\\SANKYU RPA Python Files\\DailyExchangeRate\\crossCurrencyRate_SGD.csv"  # crossCurrencyRate_SGD.csv
    #OMRRate = "0.385"
    #main()
    #main(wmPath, dailyCsvPath, crossCsvPath, crossSgdCsvPath, OMRRate)
