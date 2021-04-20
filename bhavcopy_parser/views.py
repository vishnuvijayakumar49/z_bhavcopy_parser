from django.shortcuts import render, redirect
from django.http import HttpResponse
from django.views import View
import redis, json, pdb
from django.http.response import JsonResponse

redis_conn = redis.StrictRedis(host="localhost", port=6379, db=0, decode_responses=True)

def bhavcopy_api(request):
  bhav_data_json = redis_conn.hgetall('bhav_copy_data')
  bhav_copy_list = [json.loads(bhav_data) for bhav_data in list(bhav_data_json.values())]
  return JsonResponse(bhav_copy_list, safe=False)

def search(request):
  name = request.GET.get('name').upper()
  if len(name) == 0:
    return redirect('/bhavcopy_data')
  json_data = redis_conn.hget('bhav_copy_data', name)
  response = [json.loads(json_data)] if json_data else []
  return JsonResponse(response, safe=False)

def index(request):
  return render(request, template_name='index.html')