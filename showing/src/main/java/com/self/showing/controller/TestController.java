package com.self.showing.controller;

import com.self.showing.entity.TestEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;

/**
 * created by Bonnie on 2021/4/24
 */
@Controller
@RequestMapping("/ajax")
public class TestController {

    @RequestMapping("/index")
    public String hello (Model model){
        TestEntity test = new TestEntity();
        test.setName("hello world");
        model.addAttribute("msg", test);
        model.addAttribute("test", Arrays.asList("abc", "def", "gettime"));
        return "index";
    }

//    @RequestMapping("/a1")
//    public void ajax(String name, HttpServletResponse response) throws IOException {
//        if( "admin".equals(name) ){
//            response.getWriter().print("True");
//        }else{
//            response.getWriter().print("False");
//        }
//    }

}
