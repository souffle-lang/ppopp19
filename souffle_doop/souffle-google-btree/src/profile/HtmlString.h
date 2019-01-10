/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2016, The Souffle Developers. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

#pragma once

#include "htmlCssChartist.h"
#include "htmlCssStyle.h"
#include "htmlJsChartistMin.h"
#include "htmlJsChartistPlugin.h"
#include "htmlJsMain.h"
#include "htmlJsTableSort.h"
#include "htmlJsUtil.h"
#include "htmlMain.h"
#include <sstream>
#include <string>

namespace souffle {
namespace profile {

/*
 * Class linking the html, css, and js into one html file
 * so that a data variable can be inserted in the middle of the two strings and written to a file.
 *
 */
class HtmlString {
public:
    inline std::string get_first_half() {
        std::stringstream ss;
        ss << html::htmlHeadTop << wrapCss(html::cssChartist) << wrapCss(html::cssStyle)
           << html::htmlHeadBottom << html::htmlBodyTop << "<script>";
        return ss.str();
    }
    inline std::string get_second_half() {
        std::stringstream ss;
        ss << "</script>" << wrapJs(html::jsUtil) << wrapJs(html::jsMain) << html::htmlBodyBottom;
        return ss.str();
    }

protected:
    std::string wrapCss(const std::string& css) {
        return "<style>" + css + "</style>";
    }
    std::string wrapJs(const std::string& js) {
        return "<script>" + js + "</script>";
    }
};

}  // namespace profile
}  // namespace souffle
