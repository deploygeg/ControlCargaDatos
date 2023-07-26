from flask import Flask,jsonify,request,abort,render_template
from Utilities import stats
from funciones import *
import os
import funcionesAPI as fn


app = Flask(__name__)

@app.route("/")
def hello():
    return "INICIO DE API REST DE FLASK CON APACHE24 Y PYTHON"

#funcion post para obtener el promedio de un array ingresado en un json
@app.route('/promedio',methods=["POST"])
def promedio():
    lista = request.json
    print("El resultado es :",lista["valores"])
    if not lista["valores"]:
        data ={"error":"Lista vacia"}
        #abort(404, description="Lista vacia")
        return jsonify(data)
    else:
        p=promedioArray(lista["valores"])
        data ={"promedio":p}
        return jsonify(data)
#funcion post para obtener la tendiencia
@app.route('/tendencia',methods=["POST"])
def tendencia():
    lista = request.json
    print("Lista HTTP es :",lista)
    if not lista["data_group1"]or not lista["data_group2"]:
        data ={"error":"Lista vacia"}
        #abort(404, description="Lista vacia")
        return jsonify(data)
    else:
        #r=lista
        r = stats.indicador_cambio_tendencia(lista["data_group1"], lista["data_group2"])
        print(r)
        return jsonify(r)

@app.route('/piezometros_DB')
def piezometros_API_DB():
    return render_template("piezometros_DB.html")

@app.route('/piezometros_staging')
def piezometros_staging_API():
    sensores = fn.get_all_piezometros_staging()
    #return [user.to_json() for user in users]
    return jsonify(sensores)

@app.route('/piezometros_proc')
def piezometros_proc():
    return render_template("piezometros_procesados.html")

@app.route("/piezometros")
def piezometros():
    sensores = fn.get_all_piezometros()
    #return [user.to_json() for user in users]
    return jsonify(sensores)

@app.route("/humedad")
def sensores_humedad_API():
    sensores_humedad = fn.get_all_humedad()
    return jsonify(sensores_humedad)

@app.route("/humedad_DB")
def sensores_humedad():
    #sensores_humedad = fn.get_all_humedad()
    return render_template("humedad_DB.html")
    #return jsonify(sensores_humedad)

@app.route("/gnss")
def gnss_API():
    sensores_gnss = fn.get_all_gnss()
    #return render_template("humedad_DB.html")
    return jsonify(sensores_gnss)

@app.route("/gnss_DB")
def sensores_gnss():
    return render_template("gnss_DB.html")

@app.route("/prismas")
def prismas_API():
    sensores_gnss = fn.get_all_prismas()
    #return render_template("humedad_DB.html")
    return jsonify(sensores_gnss)

@app.route("/prismas_DB")
def sensores_prismas():
    return render_template("prismas_DB.html")
#api par alos radares 
@app.route("/radares")
def radares_API():
    sensores_gnss = fn.get_all_radares()
    #return render_template("humedad_DB.html")
    return jsonify(sensores_gnss)
@app.route("/radares_DB")
def sensores_radares():
    return render_template("radares_DB.html")

@app.route("/clinoextensometros")
def clinoextensometros():
    clinoextensometros = fn.get_all_clinoextensometros()
    return jsonify(clinoextensometros)
@app.route("/clinoextensometros_DB")
def sensores_clinoextensometros():
    return render_template("clinoextensometros_DB.html")
@app.route("/piezometros_activos")
def piezometros_activos():
    sensores=fn.get_all_piezometros_activos()
    print (sensores)
    return sensores


@app.route("/piezometros_current")
def piezometros_current():
    sensores=fn.get_all_piezometros_source()
    print (sensores)
    return sensores

@app.route("/piezometros_procesados")
def piezometros_procesados():
    sensores=fn.get_all_piezometros_procesados()
    print (sensores)
    return sensores


if __name__ == "__main__":
    app.run(debug=True)