EXCEL_HEADERS = {
    "user_activity": {
        "sheetname": "地域特征",
        "map": {
            "count": "统计数量",
            "uid": "用户ID",
            "timestamp": "时间戳",
            "ip": "IP地址",
            "date": "日期",
            "geo": "地理位置"
        },
        "headers": [
            "用户ID",
            "统计数量",
            "IP地址",
            "地理位置",
            "日期",
            "时间戳",
        ]
    },
    "user_weibo_type": {
        "sheetname": "活动特征",
        "map": {
            "timestamp": "时间戳",
            "date": "日期",
            "uid": "用户ID",
            "original": "原创微博数",
            "retweet": "转发微博数",
            "comment": "评论微博数"
        },
        "headers": [
            "用户ID",
            "原创微博数",
            "转发微博数",
            "评论微博数",
            "日期",
            "时间戳",
        ]
    },
    "user_domain_topic": {
        "sheetname": "偏好特征（领域、话题）",
        "map": {
            "timestamp": "时间戳",
            "uid": "用户ID",
            "date": "日期",
            "domain_followers": "领域_转发结构",
            "domain_weibo": "领域_发帖内容",
            "domain_verified": "领域_注册信息",
            "main_domain": "用户主领域",
            "has_new_information": "信息状态",
            "topic_computer": "话题_科技类_概率",
            "topic_environment": "话题_民生类_环保_概率",
            "topic_military": "话题_军事类_概率",
            "topic_employment": "话题_民生类_就业_概率",
            "topic_house": "话题_民生类_住房_概率",
            "topic_economic": "话题_经济类_概率",
            "topic_life": "话题_其他类_概率",
            "topic_violence": "话题_政治类_暴恐_概率",
            "topic_social_security": "话题_民生类_社会保障_概率",
            "topic_politics": "话题_政治类_外交_概率",
            "topic_sports": "话题_文体类_体育_概率",
            "topic_traffic": "话题_民生类_交通_概率",
            "topic_anti_corruption": "话题_政治类_反腐_概率",
            "topic_art": "话题_文体类_娱乐_概率",
            "topic_law": "话题_民生类_法律_概率",
            "topic_education": "话题_教育类_概率",
            "topic_medicine": "话题_民生类_健康_概率",
            "topic_peace": "话题_政治类_地区和平_概率",
            "topic_religion": "话题_政治类_宗教_概率",
            "main_topic": "用户主话题"
        },
        "headers": [
            "用户ID",
            "时间戳",
            "日期",
            "信息状态",
            "用户主领域",
            "领域_转发结构",
            "领域_发帖内容",
            "领域_注册信息",
            "用户主话题",
            "话题_科技类_概率",
            "话题_民生类_环保_概率",
            "话题_军事类_概率",
            "话题_民生类_就业_概率",
            "话题_民生类_住房_概率",
            "话题_经济类_概率",
            "话题_其他类_概率",
            "话题_政治类_暴恐_概率",
            "话题_民生类_社会保障_概率",
            "话题_政治类_外交_概率",
            "话题_文体类_体育_概率",
            "话题_民生类_交通_概率",
            "话题_政治类_反腐_概率",
            "话题_文体类_娱乐_概率",
            "话题_民生类_法律_概率",
            "话题_教育类_概率",
            "话题_民生类_健康_概率",
            "话题_政治类_地区和平_概率",
            "话题_政治类_宗教_概率",
        ]
    },
    "user_text_analysis_sta": {
        "sheetname": "偏好特征（关键词、敏感词、微话题）",
        "map": {
            "keywords": "关键词词云Json串",
            "hastags": "微话题词云Json串",
            "sensitive_words": "敏感词词云Json串",
            "uid": "用户ID",
            "timestamp": "时间戳",
            "date": "日期"
        },
        "headers": [
            "用户ID",
            "日期",
            "时间戳",
            "关键词词云Json串",
            "微话题词云Json串",
            "敏感词词云Json串",
        ]
    },
    "user_influence": {
        "sheetname": "影响力特征",
        "map": {
            "uid": "用户ID",
            "importance": "重要度原始数值",
            "timestamp": "时间戳",
            "sensitivity": "敏感度原始数值",
            "influence": "影响力原始数值",
            "activity": "活跃度原始数值",
            "date": "日期",
            "importance_normalization": "重要度归一化数值",
            "influence_normalization": "影响力归一化数值",
            "activity_normalization": "活跃度归一化数值",
            "sensitivity_normalization": "敏感度归一化数值"
        },
        "headers": [
            "用户ID",
            "时间戳",
            "日期",
            "重要度原始数值",
            "敏感度原始数值",
            "影响力原始数值",
            "活跃度原始数值",
            "重要度归一化数值",
            "影响力归一化数值",
            "活跃度归一化数值",
            "敏感度归一化数值"
        ]
    },
    "user_social_contact": {
        "sheetname": "社交特征",
        "map": {
            "timestamp": "时间戳",
            "source": "被转发（评论）用户ID",
            "target": "转发（评论）用户ID",
            "source_name": "被转发（评论）用户昵称",
            "target_name": "转发（评论）用户昵称",
            "message_type": "关系类别",
            "date": "日期",
            "count": "统计数量"
        },
        "headers": [
            "被转发（评论）用户ID",
            "被转发（评论）用户昵称",
            "转发（评论）用户ID",
            "转发（评论）用户昵称",
            "时间戳",
            "日期",
            "关系类别",
            "统计数量"
        ]
    },
    "user_emotion": {
        "sheetname": "情绪特征",
        "map": {
            "timestamp": "时间戳",
            "nuetral": "中立微博数量",
            "negtive": "消极微博数量",
            "uid": "用户ID",
            "positive": "积极微博数量",
            "date": "日期"
        },
        "headers": [
            "用户ID",
            "日期",
            "时间戳",
            "中立微博数量",
            "消极微博数量",
            "积极微博数量",
        ]
    }
}

for k,v in EXCEL_HEADERS.items():
    EXCEL_HEADERS[k]['rev_map'] = dict(zip(v['map'].values(),v['map'].keys()))
# print(EXCEL_HEADERS)