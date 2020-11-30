package com.yr.server.rest;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping
public class MovieResApi {

    //获取用户实时推荐信息接口
    @RequestMapping(path = "/stream",produces = "application/json",method = RequestMethod.GET)
    @ResponseBody
    public Model getRealtimeRecommendations(@RequestParam("username") String username,@RequestParam("num") int sum, Model model){


        return null;
    }

    //提供用户离线推荐信息接口
    public Model getOfflineRecommendations(String username,Model model){
        return null;
    }

    //提供获取热门推荐电影接口
    public Model getHostRecommendations(Model model){
        return null;
    }

    //提供获取优质电影的信息接口
    public Model getRateMoreRecommendations(Model model){
        return null;
    }

    //获取最新电影的接口
    public Model getNewRecommendations(Model model){
        return null;
    }

    //模糊检索
    public Model getFuzzySearchMovies(String query,Model model){
        return null;
    }

    //提供基于名称或者描述的模糊功能的检索功能


}
